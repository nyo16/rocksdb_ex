defmodule RocksdbEx.Native do
  use Rustler, otp_app: :rocksdb_ex, crate: "rocksdbex_native"

  # When your NIF is loaded, it will override this function.
  def open(_path, _opts), do: :erlang.nif_error(:nif_not_loaded)
  def open_default(_path), do: :erlang.nif_error(:nif_not_loaded)
  def put(_db, _key, _value), do: :erlang.nif_error(:nif_not_loaded)
  def get(_db, _key), do: :erlang.nif_error(:nif_not_loaded)
  def delete(_db, _key), do: :erlang.nif_error(:nif_not_loaded)
end
