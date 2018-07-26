defmodule Vayne.Export.OpenFalcon do

  require Logger

  @behaviour Vayne.Task.Export

  @doc """
  Params below:
  * `endpoint`: endpoint to send.
  * `step`: step.
  * `tags`: tag string. Not required.
  * `metric_spec`: metric spec. Not required.
  * `server_addr`: addr to push. Not required.
  """
  @server_addr "http://127.0.0.1:1988/v1/push"
  def run(params, metrics) do
    with {:ok, new_params} <- init_params(params),
      {:ok, post} <- init_post(metrics, new_params)
    do
      if params["debug"] do
        Logger.info "[DEBUG] #{new_params["endpoint"]}: #{inspect post}"
      else
        body = Poison.encode!(post)
        ret = HTTPotion.post(new_params["server_addr"], body: body, timeout: 10_000)
        Logger.info "#{new_params["endpoint"]} [#{length(post)}] -> #{new_params["server_addr"]}, ret: #{inspect ret}"
        case ret do
          %HTTPotion.Response{status_code: 200} -> :ok
          error -> {:error, error}
        end
      end
    else
      {:error, error} -> {:error, error}
      error           -> {:error, error}
    end
  end

  def init_post({tags, metrics}, new_params), do: init_post(metrics, new_params, tags)

  def init_post(metrics, new_params) when is_list(metrics) do
    post = Enum.reduce(metrics, [], fn
      (m, acc) when is_map(m) or is_tuple(m) ->
        {:ok, post} = init_post(m, new_params)
        acc ++ post
      (_, acc) -> acc
    end)
    {:ok, post}
  end

  def init_post(metrics, new_params, tags \\ nil) when is_map(metrics) do
    specs = Application.get_env(:vayne_export_openfalcon, :openfalcon_metric_specs, [])

    metric_spec  = specs[new_params["metric_spec"]] || %{}
    allow_list   = metric_spec["allow"]
    disable_list = metric_spec["disable"]
    type_counter = metric_spec["type_counter"] || []

    post = metrics
    |> Enum.to_list
    |> Enum.filter(fn {k, _v} ->
      is_nil(allow_list) || k in allow_list
    end)
    |> Enum.filter(fn {k, _v} ->
      is_nil(disable_list) || not(k in disable_list)
    end)
    |> Enum.map(fn {k, v} ->
      type = if k in type_counter, do: "COUNTER", else: "GAUGE"
      %{
        "endpoint"    => new_params["endpoint"],
        "metric"      => k,
        "value"       => try_parse(v),
        "tags"        => make_tags(tags) || new_params["tags"],   #tags maybe merged in future
        "timestamp"   => :os.system_time(:seconds),
        "counterType" => type,
        "step"        => new_params["step"],
      }
    end)
    {:ok, post}
  end

  @doc """
  ## Examples

      iex> Vayne.Export.OpenFalcon.init_params(%{})
      {:error, "endpoint is needed"}
      iex> Vayne.Export.OpenFalcon.init_params(%{"endpoint" => "some_endpoint"})
      {:error, "step is needed"}
      iex> Vayne.Export.OpenFalcon.init_params(%{"endpoint" => "some_endpoint", "step" => 30})
      {:ok,
       %{
         "endpoint" => "some_endpoint",
         "metric_spec" => nil,
         "server_addr" => "http://127.0.0.1:1988/v1/push",
         "step" => 30,
         "tags" => ""
       }}
      iex> Vayne.Export.OpenFalcon.init_params(%{"endpoint" => "some_endpoint", "step" => 30,"tags" => "idc=xdz"})
      {:ok,
       %{
         "endpoint" => "some_endpoint",
         "metric_spec" => nil,
         "server_addr" => "http://127.0.0.1:1988/v1/push",
         "step" => 30,
         "tags" => "idc=xdz"
       }}

  """
  def init_params(params) do
    with endpoint when is_binary(endpoint)
      <- Map.get(params, "endpoint") || {:error, "endpoint is needed"},
         step when is_integer(step)
      <- Map.get(params, "step") || {:error, "step is needed"},
         tags        = Map.get(params, "tags", ""),
         metric_spec = Map.get(params, "metric_spec"),
         server_addr = params["server_addr"]
      || Application.get_env(:vayne_export_openfalcon, :openfalcon_server_addr, @server_addr)
    do
      metric_spec = if is_binary(metric_spec), do: String.to_atom(metric_spec), else: metric_spec
      {:ok, %{
        "endpoint" => endpoint, "step" => step, "tags" => tags,
        "metric_spec" => metric_spec, "server_addr" => server_addr
      }}
    end
  end

  def make_tags(tags) when is_map(tags) do
    tags
    |> Map.to_list
    |> Enum.sort
    |> Enum.map(fn {k, v} -> "#{k}=#{v}" end) |> Enum.join(",")
  end

  def make_tags(_), do: nil

  defp try_parse(value) when not is_binary(value), do: value
  defp try_parse(value) when is_binary(value) do
      case Float.parse(value) do
        :error -> value
        {v, _} -> v
        _      -> value
      end
  end
end
