#!/usr/bin/env bash
# The generated job's retry control flow, split out so its stale-build recovery
# can be exercised without compiling Rust.

run_generators_with_retry() {
  printf '%s\n' 'generator attempt 1' >>"$glog"
  run_generators
  if [ "${#gen_why[@]}" -ne 0 ]; then
    printf 'first generator pass reason: %s\n' "${gen_why[@]}" >>"$glog"
    # This confirmation still writes this checkout's units into the shared
    # target with newer mtimes, so another checkout can lose the same race in
    # reverse. Splitting manager target dirs is the complete fix; test binaries
    # are outside this generated-artifact check's scope.
    if refresh_workspace_sources >/dev/null 2>>"$glog"; then
      printf '%s\n' 'generator attempt 2' >>"$glog"
      run_generators
      echo "first generator pass reported failure or drift; refreshed this checkout's sources and retried once" \
        >>"$glog"
    else
      gen_why+=("failed to dirty workspace sources before regenerating")
    fi
  fi
}
