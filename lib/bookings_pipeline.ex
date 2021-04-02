defmodule BookingsPipeline do
  use Broadway

  def start_link(_args) do
    # Broadway uses this configuration to dynamically build and start all parts
    # of the data ingestion pipeline for us
    opts = [
      # prefix when naming processes
      name: BookingsPipeline, 
      # configuration for source of events
      producer: [
        module: {BroadwayRabbitMQ.Producer,
          queue: "bookings_queue",
          declare: [durable: true],
          on_failure: :reject_and_requeue,
          qos: [prefetch_count: 100],
          connection: "amqps://sjbudugx:g2E8j56gcMXaJDl092fKM6hU8hZxLh9u@shrimp.rmq.cloudamqp.com/sjbudugx",
        },
      ], 
      # configuration for stage processes that receive messages and do most
      # of the work
      processors: [default: []],
      batchers: [
        default: [batch_size: 50],
        cinema: [batch_size: 75],
        musical: [] #defaults to :batch_size 100
      ],
    ]

    Broadway.start_link(__MODULE__, opts)
  end

  def prepare_messages(messages, _context) do
    # Parse messages and convert to a map
    msgs =
      Enum.map(messages, fn msg ->
        Broadway.Message.update_data(msg, fn data ->
          [event, user_id] = String.split(data, ",")
          %{event: event, user_id: user_id}
        end)
      end)

    users = Tickets.users_by_ids(Enum.map(msgs, &(&1.data.user_id)))

    Enum.map(msgs, fn msg ->
      Broadway.Message.update_data(msg, fn data ->
        user = Enum.find(users, &(&1.id == data.user_id))
        Map.put(data, :user, user)
      end)
    end)
  end

  def handle_message(_processor, message, _context) do
    if Tickets.tickets_available?(message.data.event) do
      case message do
        %{data: %{event: "cinema"}} = message ->
          Broadway.Message.put_batcher(message, :cinema)
        %{data: %{event: "musical"}} = message ->
          Broadway.Message.put_batcher(message, :musical)
        msg ->
          msg
      end
    else
      Broadway.Message.failed(message, "bookings-closed")
    end
  end

  # def handle_batch(:cinema, messages, batch_info, _context) do
    # Process cinema bookings separately...
  # end

  def handle_batch(_batcher, messages, batch_info, _context) do
    # Process all other bookings...
    IO.puts("#{inspect(self())} Batch #{batch_info.batcher} #{batch_info.batch_key}")

    messages
    |> Tickets.insert_all_tickets()
    |> Enum.each(fn message ->
      channel = message.metadata.amqp_channel
      payload = "email,#{message.data.user.email}"
      AMQP.Basic.publish(channel, "", "notifications_queue", payload)
    end)

    messages
  end

  def handle_failed(messages, _context) do
    IO.inspect(messages, label: "Failed messages")
    Enum.map(messages, fn %{status: {:failed, "bookings-closed"}} = msg ->
      Broadway.Message.configure_ack(msg, on_failure: :reject)
      msg ->
        msg
    end)
  end
end
