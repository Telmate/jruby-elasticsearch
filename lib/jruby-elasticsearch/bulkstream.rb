require "jruby-elasticsearch/namespace"
require "thread"

class ElasticSearch::BulkStream
  # Create a new bulk stream. This allows you to send
  # index and other bulk events asynchronously and use
  # the bulk api in ElasticSearch in a streaming way.
  #
  # The 'queue_size' is the maximum size of unflushed
  # requests. If the queue reaches this size, new requests
  # will block until there is room to move.
  def initialize(client, queue_size=10, flush_interval=1, flushers=1)
    @client = client
    @queue_size = queue_size
    @queue = SizedQueue.new(@queue_size * (flushers * 2)) # allow a back-buffer of requests
    @flush_interval = flush_interval
    @bulkthreads = []
    @flush_mutex = Mutex.new
    flushers.times {
      @bulkthreads << Thread.new { run }
    }
  end # def initialize

  # See ElasticSearch::BulkRequest#index for arguments.
  public
  def index(*args)
    # TODO(sissel): It's not clear I need to queue this up, I could just
    # call BulkRequest#index() and when we have 10 or whatnot, flush, but
    # Queue gives us a nice blocking mechanism anyway.
    @queue << [:index, *args]
  end # def index

  def partial_update(*args)
    @queue << [:partial_update, *args]
  end # def partial_update

  # The stream runner.
  private
  def run
    # TODO(sissel): Make a way to shutdown this thread.
    while true
      begin
        requests = []
        if @queue.size >= @queue_size
          # queue full, flush now.
          flush
        else
          # Not full, so sleep and flush anyway.
          sleep(@flush_interval)
          flush
        end

        if @stop and @queue.size == 0
          # Queue empty and it's time to stop.
          break
        end
      rescue => err
        # TODO log this better
        $stderr.puts err.inspect
        $stderr.puts err.backtrace
      end
    end # while true
  end # def run

  # Stop the stream
  public
  def stop
    @queue.clear if @queue.empty? # wake up the waiters
    @queue << nil
    @stop = true
  end # def stop

  # Flush the queue right now. This will block until the
  # bulk request has completed.
  public
  def flush
    bulk = @client.bulk

    @flush_mutex.synchronize {
      flush_one = proc do
        # block if no data.
        method, *args = @queue.pop
        return if args.nil? # probably we are now stopping.
        bulk.send(method, *args)
      end

      flush_one.call # will wait on pop if empty

      1.upto([@queue.size, @queue_size - 1].min) do
        flush_one.call
      end
    }

    # Block until this finishes
    bulk.execute!
  end # def flush
end # class ElasticSearch::BulkStream
