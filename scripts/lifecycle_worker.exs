# Disposable bootstrap: actual application callback, scheduler and executor.
Code.prepend_paths(Path.wildcard("/app/elixir_app/_build/prod/lib/*/ebin") |> Enum.reject(&String.ends_with?(&1, "/elixir_app/ebin")))
File.mkdir_p!("/tmp/lifecycle-ebin")
{:ok, _, %{compile_warnings: [], runtime_warnings: []}} = Kernel.ParallelCompiler.compile_to_path(Path.wildcard("/src/elixir_app/lib/**/*.ex"), "/tmp/lifecycle-ebin", return_diagnostics: true)
File.cp!("/app/elixir_app/_build/prod/lib/elixir_app/ebin/elixir_app.app", "/tmp/lifecycle-ebin/elixir_app.app")
Code.prepend_path("/tmp/lifecycle-ebin")
Application.ensure_all_started(:elixir_app, :permanent)
Process.sleep(:infinity)
