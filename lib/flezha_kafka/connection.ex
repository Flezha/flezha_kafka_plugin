defmodule FlezhaKafka.Connection do
  @moduledoc """
  What every Kafka provider shares about its connection: the option rows that
  describe it, the parsing of `hosts`, the SASL credential and its redaction,
  and the client options a group member connects with.

  The credential the user gave is held in the transient row `auth_secret`,
  which is kept in memory and never persisted, and the prepared `auth` carries
  `**redacted**` in place of the password. `resolve_auth/1` builds the
  `KafkaEx.Auth.Config` a client authenticates with, whose `Inspect` redacts
  the password, at every client start, so `sasl_password_env` is read again on
  each reconnect.
  """

  alias Flezha.Plugin.ProviderBehaviour
  alias KafkaEx.Auth.Config, as: AuthConfig

  import ProviderBehaviour, only: [parse_enum: 3, refuse_value: 2]

  # The keys and the mechanisms `KafkaEx.Auth.Config` accepts.
  @auth_keys [:mechanism, :username, :password, :mechanism_opts]
  @auth_mechanisms [:plain, :scram, :oauthbearer, :msk_iam]

  @redacted "**redacted**"

  @rows [
    hosts: [
      type: :any,
      required: true,
      doc: "`{host, port}` list, or a comma-separated string"
    ],
    auth: [type: :any, doc: "SASL configuration, as a `KafkaEx.Auth.Config` or a map"],
    # The credential as given, held in memory so a client start and every
    # reconnect on this node authenticate with the real secret while the
    # stored opts carry only the placeholder. Never persisted, so a handover
    # to another node resolves from `sasl_password_env` or from nothing.
    auth_secret: [type: :any, internal: true, transient: true],
    sasl_password_env: [
      type: :string,
      doc: "Environment variable holding the SASL password, preferred over an inline one"
    ],
    use_ssl: [type: :bool, doc: "TLS to the brokers"],
    ssl_options: [type: :any, doc: "Erlang `:ssl` options for that connection"]
  ]

  @doc "The connection's option rows, for a provider's declaration."
  @spec rows() :: keyword()
  def rows(), do: @rows

  @doc """
  `opts` with `hosts` parsed, the credential moved into `auth_secret` and the
  `auth` password replaced by the placeholder. A plugin prepared again from
  these opts keeps the secret it holds.
  """
  @spec prepare(map()) :: {:ok, map()} | {:error, term()}
  def prepare(opts) do
    with {:ok, hosts} <- parse_hosts(opts.hosts) do
      {:ok, redact(%{opts | hosts: hosts, auth_secret: opts.auth_secret || opts.auth})}
    end
  end

  @doc """
  The SASL credential of prepared `opts` as a `KafkaEx.Auth.Config`, or `nil`.
  `sasl_password_env` overrides an inline password. Called at every client
  (re)start, so a rotated variable takes effect on the next reconnect; the keys
  and the mechanism were closed by `convert_auth/1` at preparation.
  """
  @spec resolve_auth(map()) :: AuthConfig.t() | nil
  def resolve_auth(opts) do
    case opts.auth_secret || opts.auth do
      nil ->
        nil

      %AuthConfig{} = config ->
        override_password(config, opts)

      %{} = map ->
        map
        |> Map.put_new(:mechanism, :plain)
        |> AuthConfig.new()
        |> override_password(opts)
    end
  end

  @doc "The broker client options of prepared `opts`, the credential resolved."
  @spec client_opts(map()) :: keyword()
  def client_opts(opts),
    do: [
      uris: opts.hosts,
      use_ssl: opts.use_ssl || false,
      ssl_options: opts.ssl_options || [],
      auth: resolve_auth(opts)
    ]

  @doc """
  The `auth` row's conversion, for a provider's `convert_value/2`.
  `KafkaEx.Auth.Config.new/1` takes only its four keys and drops the rest, so a
  misspelled key is refused here rather than read as a missing credential, and
  a mechanism outside its set is refused rather than raised inside the client
  start the registry re-runs on every reconnect.
  """
  @spec convert_auth(term()) :: {:ok, AuthConfig.t() | map()} | {:error, term()}
  def convert_auth(%AuthConfig{} = config), do: {:ok, config}
  def convert_auth(%{} = map), do: Enum.reduce_while(map, {:ok, %{}}, &put_auth_opt/2)
  def convert_auth(value), do: refuse_value(:auth, value)

  defp put_auth_opt({key, value}, {:ok, acc}) do
    with {:ok, field} <- parse_enum(key, "auth key", @auth_keys),
         {:ok, value} <- auth_value(field, value) do
      {:cont, {:ok, Map.put(acc, field, value)}}
    else
      {:error, _} = err -> {:halt, err}
    end
  end

  defp auth_value(:mechanism, value), do: parse_enum(value, "auth mechanism", @auth_mechanisms)
  defp auth_value(_field, value), do: {:ok, value}

  defp override_password(%AuthConfig{} = config, opts) do
    case opts.sasl_password_env do
      env when is_binary(env) and env != "" -> %{config | password: System.get_env(env)}
      _ -> config
    end
  end

  defp redact(%{auth: %AuthConfig{password: p} = config} = opts) when is_binary(p) and p != "",
    do: %{opts | auth: %{config | password: @redacted}}

  defp redact(%{auth: %{password: p} = map} = opts) when is_binary(p) and p != "",
    do: %{opts | auth: %{map | password: @redacted}}

  defp redact(opts), do: opts

  defp parse_hosts(hosts) when is_binary(hosts) do
    hosts
    |> String.split(",")
    |> Enum.reduce_while({:ok, []}, fn host, {:ok, acc} ->
      with [h, port] <- String.split(host, ":"),
           {port, ""} <- Integer.parse(port) do
        {:cont, {:ok, [{h, port} | acc]}}
      else
        _ -> {:halt, refuse_value(:hosts, hosts)}
      end
    end)
    |> case do
      {:ok, parsed} -> {:ok, Enum.reverse(parsed)}
      refusal -> refusal
    end
  end

  defp parse_hosts(hosts), do: {:ok, hosts}
end
