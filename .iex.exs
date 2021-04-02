send_messages = fn num_messages ->
  {:ok, conn} = AMQP.Connection.open("amqps://sjbudugx:g2E8j56gcMXaJDl092fKM6hU8hZxLh9u@shrimp.rmq.cloudamqp.com/sjbudugx")

  {:ok, channel} = AMQP.Channel.open(conn)

  Enum.each(1..num_messages, fn _ ->
    event = Enum.random(["cinema", "musical", "play"])
    user_id = Enum.random(1..3)
    AMQP.Basic.publish(channel, "", "bookings_queue", "#{event},#{user_id}")
  end)

  AMQP.Connection.close(conn)
end
