defmodule Weddell.Consumer do
  alias Weddell.{Message,
                 Client.Subscriber}

  @typedoc "Message handler response option"
  @type response_option :: {:ack, [Message.t]} |
                           {:delay, [Subscriber.Stream.message_delay]}

  @typedoc "Option values used when connecting clients"
  @type response_options:: [response_option]

  @callback handle_messages(messages :: [Message.t]) ::
    {:ok, response_options} | :error

  defmacro __using__(_opts) do
    quote do
      require Logger
      @behaviour Weddell.Consumer

      def child_spec(subscription) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [subscription]},
          restart: :permanent,
          shutdown: 5000,
          type: :worker,
        }
      end

      def start_link(subscription) do
        GenServer.start_link(__MODULE__, [subscription])
      end

      def init(subscription) do
        IO.inspect :init
        IO.inspect :init
        stream =
          Weddell.client()
          |> Subscriber.Stream.open(subscription)
        GenServer.cast(self(), :listen)
        {:ok, stream}
      end

      def handle_cast(:listen, stream) do
        IO.inspect :listen3
        IO.inspect :listen4
        IO.inspect stream.subscription
        IO.inspect :listen2
        IO.inspect :listen2
        stream
        |> Subscriber.Stream.recv()
        |> (fn d ->
          IO.inspect :aaaa
          IO.inspect :aaaa
          IO.inspect d
          {:ok, arr} = d.enum
          arr
        end).()
        |> Enum.each(fn messages ->
          IO.inspect :messages
          IO.inspect :messages
          IO.inspect messages
          dispatch(messages, stream)
        end)
        {:stop, :stream_closed, stream}
      end

      defp dispatch(messages, stream) do
        m = case messages do
          {:error, _} -> []
          {:ok, m} ->
          m.received_messages
        end
        IO.inspect :apple2
        IO.inspect :apple2
        IO.inspect :apple2
        IO.inspect m

        case handle_messages(m) do
          {:ok, opts} ->
            IO.inspect :opts
            IO.inspect :opts
            IO.inspect opts
            Logger.debug fn ->
              ack = Keyword.get(opts, :ack, [])
              delay = Keyword.get(opts, :delay, [])
              {"Sending message response",
                ack_count: length(ack),
                delay_count: length(delay),
                no_response_count: length(m) - length(ack) + length(delay)}
            end
            stream
            |> Subscriber.Stream.send(opts)
        end
      end
    end
  end
end
