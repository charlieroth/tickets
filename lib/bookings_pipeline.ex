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
          connection: "amqps://sjbudugx:g2E8j56gcMXaJDl092fKM6hU8hZxLh9u@shrimp.rmq.cloudamqp.com/sjbudugx",
          declare: [durable: true],
          on_failure: :reject_and_requeue,
        },
      ], 
      # configuration for stage processes that receive messages and do most
      # of the work
      processors: [default: []] 
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

  def handle_message(_processor, msg, _context) do
    %{data: %{event: event, user: user}} = msg

    if Tickets.tickets_available?(event) do
      Tickets.create_ticket(user, event)
      Tickets.send_email(user)
      IO.inspect(msg, label: "Message")
    else
      Broadway.Message.failed(msg, "bookings-closed")
    end
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
