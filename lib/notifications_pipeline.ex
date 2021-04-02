defmodule NotificationsPipeline do
  use Broadway

  def start_link(_args) do
    # Broadway uses this configuration to dynamically build and start all parts
    # of the data ingestion pipeline for us
    opts = [
      # prefix when naming processes
      name: NotificationsPipeline, 
      # configuration for source of events
      producer: [
        module: {BroadwayRabbitMQ.Producer,
          queue: "notifications_queue",
          on_failure: :reject_and_requeue,
          declare: [durable: true],
          qos: [prefetch_count: 100],
          connection: "amqps://sjbudugx:g2E8j56gcMXaJDl092fKM6hU8hZxLh9u@shrimp.rmq.cloudamqp.com/sjbudugx",
        },
      ], 
      # configuration for stage processes that receive messages and do most
      # of the work
      processors: [
        default: []
      ],
      batchers: [
        email: [concurrency: 5, batch_timeout: 10_000],
      ],
    ]

    Broadway.start_link(__MODULE__, opts)
  end

  def prepare_messages(messages, _context) do
    Enum.map(messages, fn message ->
      Broadway.Message.update_data(message, fn data ->
        [type, recipient] = String.split(data, ",")
        %{type: type, recipient: recipient}
      end)
    end)
  end
  
  def handle_message(_processor, message, _context) do
    message 
    |> Broadway.Message.put_batcher(:email)
    |> Broadway.Message.put_batch_key(message.data.recipient)
  end

  def handle_batch(_batcher, messages, batch_info, _context) do
    IO.puts("#{inspect(self())} Batch #{batch_info.batcher} #{batch_info.batch_key}")
    # Send an email digest to the user with all information...
    messages
  end
end
