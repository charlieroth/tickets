defmodule Tickets.MixProject do
  use Mix.Project

  def project do
    [
      app: :tickets,
      version: "0.1.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:lager, :logger],
      mod: {Tickets.Application, []}
    ]
  end

  defp deps do
    [
      {:broadway, "~> 0.6.2"},
      {:broadway_rabbitmq, "~> 0.6.5"}
    ]
  end
end
