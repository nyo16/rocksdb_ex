defmodule RocksdbExTest do
  use ExUnit.Case
  doctest RocksdbEx

  test "greets the world" do
    assert RocksdbEx.hello() == :world
  end
end
