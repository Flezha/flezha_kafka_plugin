defmodule FlezhaKafka.Connection do
  @moduledoc """
  What every Kafka provider shares about its connection: the option rows that
  describe it, the parsing of `hosts`, the SASL credential, and the client
  options a group member connects with.

  The SASL password is the shared credential row (`password`, `password_env`):
  a password written inside `auth` is moved into `password`, which is held in
  memory and never persisted while the prepared opts carry `**redacted**`.
  `resolve_auth/1` builds the `KafkaEx.Auth.Config` a client authenticates
  with, whose `Inspect` redacts the password, at every client start, so
  `password_env` is read again on each reconnect.
  """

  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias KafkaEx.Auth.Config, as: AuthConfig

  import ProviderBehaviour, only: [parse_enum: 3, refuse_value: 2]

  # The keys and the mechanisms `KafkaEx.Auth.Config` accepts.
  @auth_keys [:mechanism, :username, :password, :mechanism_opts]
  @auth_mechanisms [:plain, :scram, :oauthbearer, :msk_iam]

  @rows [
          hosts: [
            type: :names,
            required: true,
            doc: "`host:port` names, comma-separated or as a list"
          ],
          auth: [type: :map, doc: "SASL configuration, as a `KafkaEx.Auth.Config` or a map"]
        ] ++
          Options.secret(:password, "SASL password") ++
          [
            use_ssl: [type: :bool, doc: "TLS to the brokers"],
            ssl_options: [type: :list, doc: "Erlang `:ssl` options for that connection"]
          ]

  @doc "The connection's option rows, for a provider's declaration."
  @spec rows() :: keyword()
  def rows(), do: @rows

  @doc """
  `opts` with `hosts` parsed and the password, written inside `auth` or as
  `password`, held and replaced by the placeholder. A plugin prepared again
  from these opts keeps the secret it holds.
  """
  @spec prepare(map()) :: {:ok, map()} | {:error, term()}
  def prepare(opts) do
    with {:ok, hosts} <- parse_hosts(opts.hosts) do
      {:ok, opts |> Map.put(:hosts, hosts) |> lift_password() |> Options.hold_secret(:password)}
    end
  end

  # A password inside `auth` is the `password` row's, so it is held once.
  defp lift_password(%{auth: %{password: password} = auth} = opts) when is_binary(password),
    do: %{opts | auth: %{auth | password: nil}, password: Map.get(opts, :password) || password}

  defp lift_password(opts), do: opts

  @doc """
  The SASL credential of prepared `opts` as a `KafkaEx.Auth.Config`, or `nil`,
  with the password resolved now. Called at every client (re)start, so a
  rotated variable takes effect on the next reconnect; the keys and the
  mechanism were closed by `convert_auth/1` at preparation.
  """
  @spec resolve_auth(map()) :: AuthConfig.t() | nil
  def resolve_auth(%{auth: nil}), do: nil

  def resolve_auth(opts) do
    password = Options.secret_fun(opts, :password).()

    case opts.auth do
      %AuthConfig{} = config ->
        put_password(config, password)

      %{} = map ->
        map
        |> Map.put_new(:mechanism, :plain)
        |> put_password(password)
        |> AuthConfig.new()
    end
  end

  defp put_password(auth, nil), do: auth
  defp put_password(auth, password), do: Map.put(auth, :password, password)

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
  The `hosts` row's conversion, for a provider's `convert_value/2`. A prepared
  plugin's stored options carry `hosts` as `{host, port}` tuples, and preparing
  it again converts them again, so that list is kept as it is; anything else is
  converted by the `names` row.
  """
  @spec convert_hosts(term()) :: {:ok, term()} | {:error, term()}
  def convert_hosts(hosts) do
    if is_list(hosts) and hosts != [] and Enum.all?(hosts, &host_tuple?/1),
      do: {:ok, hosts},
      else: Options.convert_row(:hosts, hosts, @rows)
  end

  defp host_tuple?({host, port}), do: is_binary(host) and is_integer(port)
  defp host_tuple?(_), do: false

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

  defp parse_hosts(hosts) when is_list(hosts) do
    hosts
    |> Enum.reduce_while({:ok, []}, fn host, {:ok, acc} ->
      case parse_host(host) do
        {:ok, parsed} -> {:cont, {:ok, [parsed | acc]}}
        refusal -> {:halt, refusal}
      end
    end)
    |> case do
      {:ok, parsed} -> {:ok, Enum.reverse(parsed)}
      refusal -> refusal
    end
  end

  defp parse_hosts(hosts), do: refuse_value(:hosts, hosts)

  defp parse_host({_host, _port} = host), do: {:ok, host}

  defp parse_host(host) when is_binary(host) do
    with [h, port] when h != "" <- String.split(host, ":"),
         {port, ""} <- Integer.parse(port) do
      {:ok, {h, port}}
    else
      _ -> refuse_value(:hosts, host)
    end
  end

  defp parse_host(host), do: refuse_value(:hosts, host)
end
