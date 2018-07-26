defmodule Vayne.Export.OpenFalconTest do
  use ExUnit.Case, async: false
  doctest Vayne.Export.OpenFalcon
  alias Vayne.Export.OpenFalcon

  @test_addr "http://test_addr/v1/push"
  test "server_addr from application env" do
    Application.put_env(:vayne_export_openfalcon, :openfalcon_server_addr, @test_addr)
    on_exit "remove env", fn -> Application.delete_env(:vayne_export_openfalcon, :openfalcon_server_addr) end

    ret = OpenFalcon.init_params(%{"endpoint" => "some_endpoint", "step" => 30})
    assert {:ok, %{
      "endpoint" => "some_endpoint",
      "metric_spec" => nil,
      "server_addr" => "http://test_addr/v1/push",
      "step" => 30,
      "tags" => ""
    }} ==  ret
  end

  test "server_addr from params" do
    Application.put_env(:vayne_export_openfalcon, :openfalcon_server_addr, @test_addr)
    on_exit "remove env", fn -> Application.delete_env(:vayne_export_openfalcon, :openfalcon_server_addr) end

    ret = OpenFalcon.init_params(%{"endpoint" => "some_endpoint", "step" => 30, "server_addr" => "http://params_addr/v1/push"})

    assert {:ok, %{
      "endpoint" => "some_endpoint",
      "metric_spec" => nil,
      "server_addr" => "http://params_addr/v1/push",
      "step" => 30,
      "tags" => ""
    }} == ret
  end


  @test_specs [
    test: %{
      "type_counter" => ~w(baz),
      "allow" => ~w(foo bar baz),
      "disable" => ~w(foo)
    }
  ]

  test "test specs" do

    Application.put_env(:vayne_export_openfalcon, :openfalcon_metric_specs, @test_specs)
    on_exit "remove env", fn -> Application.delete_env(:vayne_export_openfalcon, :openfalcon_metric_specs) end

    {:ok, params} = OpenFalcon.init_params(%{
      "endpoint"    => "some_endpoint",
      "step"        => 30,
      "metric_spec" => "test"
    })

    metric = %{"foo" => 1, "bar" => 2, "baz" => 3}

    {:ok, ret} = OpenFalcon.init_post(metric, params)
    metrics = Enum.map(ret, &(Map.delete(&1, "timestamp")))

    assert metrics == [
       %{
         "counterType" => "GAUGE",
         "endpoint" => "some_endpoint",
         "metric" => "bar",
         "step" => 30,
         "tags" => "",
         "value" => 2
       },
       %{
         "counterType" => "COUNTER",
         "endpoint" => "some_endpoint",
         "metric" => "baz",
         "step" => 30,
         "tags" => "",
         "value" => 3
       }
     ]
  end

  test "tuple with tag" do
    params = %{"endpoint" => "some_endpoint", "step" => 30}
    metric = {%{"idc" => "aa"}, %{"bar" => 2, "baz" => 3}}

    {:ok, stat} = OpenFalcon.init_params(params)

    {:ok, post} = OpenFalcon.init_post(metric, stat)

    metrics = Enum.map(post, &(Map.delete(&1, "timestamp")))

    assert metrics == [
       %{
         "counterType" => "GAUGE",
         "endpoint" => "some_endpoint",
         "metric" => "bar",
         "step" => 30,
         "tags" => "idc=aa",
         "value" => 2
       },
       %{
         "counterType" => "GAUGE",
         "endpoint" => "some_endpoint",
         "metric" => "baz",
         "step" => 30,
         "tags" => "idc=aa",
         "value" => 3
       }
     ]
  end

  test "test list" do
    params = %{"endpoint" => "some_endpoint", "step" => 30}

    metric = [
      %{"foo" => 10},
      {%{"idc" => "aa"}, %{"bar" => 2, "baz" => 3}},
      {%{"idc" => "bb"}, %{"bar" => 4, "baz" => 6}},
    ]

    {:ok, stat} = OpenFalcon.init_params(params)

    {:ok, post} = OpenFalcon.init_post(metric, stat)

    metrics = Enum.map(post, &(Map.delete(&1, "timestamp")))

    assert metrics == [
      %{
        "counterType" => "GAUGE",
        "endpoint" => "some_endpoint",
        "metric" => "foo",
        "step" => 30,
        "tags" => "",
        "value" => 10
      },
      %{
        "counterType" => "GAUGE",
        "endpoint" => "some_endpoint",
        "metric" => "bar",
        "step" => 30,
        "tags" => "idc=aa",
        "value" => 2
      },
      %{
        "counterType" => "GAUGE",
        "endpoint" => "some_endpoint",
        "metric" => "baz",
        "step" => 30,
        "tags" => "idc=aa",
        "value" => 3
      },
      %{
        "counterType" => "GAUGE",
        "endpoint" => "some_endpoint",
        "metric" => "bar",
        "step" => 30,
        "tags" => "idc=bb",
        "value" => 4
      },
      %{
        "counterType" => "GAUGE",
        "endpoint" => "some_endpoint",
        "metric" => "baz",
        "step" => 30,
        "tags" => "idc=bb",
        "value" => 6
      }
    ]
  end

  test "post success" do

    bypass = Bypass.open(port: 19888)

    Application.put_env(:vayne_export_openfalcon, :openfalcon_server_addr, "http://127.0.0.1:19888/v1/push")
    Application.put_env(:vayne_export_openfalcon, :openfalcon_metric_specs, @test_specs)

    on_exit "remove env", fn ->
      Application.delete_env(:vayne_export_openfalcon, :openfalcon_server_addr)
      Application.delete_env(:vayne_export_openfalcon, :openfalcon_metric_specs)
    end

    params = %{"endpoint" => "some_endpoint", "step" => 30, "metric_spec" => "test"}

    metric = %{"foo" => 1, "bar" => 2, "baz" => 3}

    result = [
      %{
        "counterType" => "GAUGE",
        "endpoint" => "some_endpoint",
        "metric" => "bar",
        "step" => 30,
        "tags" => "",
        "value" => 2
      },
      %{
        "counterType" => "COUNTER",
        "endpoint" => "some_endpoint",
        "metric" => "baz",
        "step" => 30,
        "tags" => "",
        "value" => 3
      }
    ]

    Bypass.expect bypass, fn conn ->

      assert "POST"     == conn.method
      assert "/v1/push" == conn.request_path

      {:ok, body, _} = Plug.Conn.read_body(conn)
      body
      |> Poison.decode!()
      |> Enum.each(fn obj ->
        obj = Map.delete(obj, "timestamp")
        assert obj in result
      end)

      Plug.Conn.resp(conn, 200, "")
    end

    assert :ok = OpenFalcon.run(params, metric)
  end

end
