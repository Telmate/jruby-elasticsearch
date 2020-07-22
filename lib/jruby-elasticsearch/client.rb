require "java"
require "jruby-elasticsearch/namespace"
require "jruby-elasticsearch/indexrequest"
require "jruby-elasticsearch/searchrequest"
require "jruby-elasticsearch/templaterequest"

class ElasticSearch::Client
  class Error < StandardError; end
  class ConfigurationError < Error; end

  attr_reader :logger 
  
  # Creates a new ElasticSearch client.
  #
  # options:
  # :type => [:local, :node] - :local will create a process-local
  #   elasticsearch instances
  # :host => "hostname" - the hostname to connect to.
  # :port => 9200 - the port to connect to
  # :cluster => "clustername" - the cluster name to use
  # :node_name => "node_name" - the node name to use when joining the cluster
  def initialize(options={})
    @logger = org.elasticsearch.common.logging.ESLoggerFactory.getLogger(self.class.name)
    builder = org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder
    builder.put("node.client", true)

    # The client doesn't need to serve http
    builder.put("http.enabled", false)

    # Use unicast discovery a host is given
    if !options[:host].nil?
      port = (options[:port] or "9300")
      builder.put("discovery.zen.ping.multicast.enabled", false)
      if port =~ /^\d+-\d+$/
        # port ranges are 'host[port1-port2]' according to
        # http://www.elasticsearch.org/guide/reference/modules/discovery/zen/
        # However, it seems to only query the first port.
        # So generate our own list of unicast hosts to scan.
        range = Range.new(*port.split("-"))
        hosts = range.collect { |p| "#{options[:host]}:#{p}" }.join(",")
        builder.put("discovery.zen.ping.unicast.hosts", hosts)
      else
        # only one port, not a range.
        logger.info "PORT SETTINGS #{options[:host]}:#{port}", nil
        builder.put("discovery.zen.ping.unicast.hosts",
                             "#{options[:host]}:#{port}")
      end
    end

    if options[:bind_host] 
      if options[:publish_host]
        builder.put('network.bind_host', options[:bind_host])
      else
        builder.put('network.host', options[:bind_host])
      end
    end

    if options[:publish_host]
      builder.put('network.publish_host', options[:publish_host])
    end

    if options[:bind_port]
      builder.put('transport.tcp.port', options[:bind_port])
    end

    if options[:node_name]
      builder.put('node.name', options[:node_name])
    end

    if !options[:cluster].nil?
      builder.put('cluster.name', options[:cluster])
    end
    @options = options
    @builder = builder
    @client_mutex = Mutex.new
    connect(@options, @builder)
    at_exit {
      close
    }
  end # def initialize

  def connect(options, builder)
    case options[:type]
      when :transport
        @client = org.elasticsearch.client.transport.TransportClient.new(builder.build)
        if options[:host]
          @client.addTransportAddress(
            org.elasticsearch.common.transport.InetSocketTransportAddress.new(
              options[:host], options[:port] || 9300
            )
          )
        else
          raise ConfigurationError, "When using a transport client, you must give a :host setting to ElasticSearch::Client.new. Otherwise, I don't know what elasticsearch servers talk to."
        end
      else
        nodebuilder = org.elasticsearch.node.NodeBuilder.nodeBuilder
        @node = nodebuilder.settings(builder).node
        @client = @node.client
    end
  end # def connect
  
  def jclient
    @client_mutex.synchronize { return @client }
  end
  
  def reconnect
    @client_mutex.synchronize do
      close
      connect(@options, @builder)
    end
  end # def reconnect

  def close
    if @node
      begin
        @node.close
        @client.close
        @node = nil
        @client = nil
      rescue Exception => err
        puts err.inspect
        logger.error "Errror on node close", err
      end
    end
  end

  # Get a new BulkRequest for sending multiple updates to elasticsearch in one
  # request.
  public
  def bulk
    return ElasticSearch::BulkRequest.new(jclient)
  end # def bulk

  public
  def bulkstream(queue_size=10, flush_interval=1, flushers=1)
    return ElasticSearch::BulkStream.new(self, queue_size, flush_interval,flushers)
  end # def bulk

  # Index a new document
  #
  # args:
  #   index: the index name
  #   type: the type name
  #   id: (optional) the id of the document
  #   data: (optional) the data for this document
  #   &block: (optional) optional block for using the DSL to add data
  #
  # Returns an ElasticSearch::IndexRequest instance.
  #
  # Example w/ DSL:
  #
  #     request = client.index("foo", "logs") do
  #       filename "/var/log/message"
  #       mesage "hello world"
  #       timestamp 123456
  #     end
  #
  #     request.execute!
  def index(index, type, id=nil, data={}, &block)
    # Permit 'id' being omitted entirely.
    # Thus a call call: index("foo", "bar", somehash) is valid.
    if id.is_a?(Hash)
      data = id
      id = nil
    end

    indexreq = ElasticSearch::IndexRequest.new(jclient, index, type, id, data)
    if block_given?
      indexreq.instance_eval(&block)
    end
    return indexreq
  end # def index

  # Search for data.
  # If a block is given, it is passed to SearchRequest#with so you can
  # more easily configure the search, like so:
  #
  #   search = client.search("foo") do
  #     query("*")
  #     histogram("field", 1000)
  #   end
  #
  #   The context of the block is of the SearchRequest object.
  public
  def search(&block)
    searchreq = ElasticSearch::SearchRequest.new(jclient)
    if block_given?
      searchreq.with(&block)
    end
    return searchreq
  end # def search

  def cluster
    return jclient.admin.cluster
  end

  def node
    return jclient.admin.cluster
  end
end # class ElasticSearch::Client

