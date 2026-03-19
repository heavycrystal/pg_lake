#!/bin/bash
set -euo pipefail

# build-pg-lake-copy.sh
#
# Builds pg_lake_copy and all its dependencies (including pgduck_server with
# DuckDB) on a bare Ubuntu/Debian machine, WITHOUT the Azure SDK.
#
# This eliminates the ~20-minute vcpkg bootstrap + Azure SDK compilation.
# S3 support works via DuckDB's built-in httpfs+aws extensions.

# ---------- configuration ----------
PG_VERSION="${PG_VERSION:-17}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"

# Determine script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() { echo -e "${BLUE}==>${NC} ${GREEN}$1${NC}"; }
print_info()   { echo -e "${BLUE}==>${NC} $1"; }
print_error()  { echo -e "${RED}Error:${NC} $1" >&2; }

# ---------- helper: elapsed time ----------
SECONDS=0
print_elapsed() {
    local mins=$((SECONDS / 60))
    local secs=$((SECONDS % 60))
    print_info "Elapsed: ${mins}m ${secs}s"
}

# ---------- step 1: install PGDG and system packages ----------
install_system_deps() {
    print_header "Installing system dependencies for PostgreSQL ${PG_VERSION}"

    # PGDG repository
    if ! apt-cache policy 2>/dev/null | grep -q apt.postgresql.org; then
        print_info "Adding PGDG apt repository"
        sudo apt-get install -y curl ca-certificates gnupg lsb-release
        curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg 2>/dev/null || true
        echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
        sudo apt-get update
    fi

    sudo apt-get install -y \
        build-essential cmake ninja-build git pkg-config \
        postgresql-${PG_VERSION} postgresql-server-dev-${PG_VERSION} \
        libssl-dev libcurl4-openssl-dev \
        libjansson-dev libsnappy-dev liblzma-dev \
        liblz4-dev zlib1g-dev \
        libreadline-dev libxml2-dev libxslt1-dev \
        libpq-dev libkrb5-dev \
        libselinux1-dev libzstd-dev libpam0g-dev libnuma-dev

    # Ensure pg_config from our target version is first on PATH
    export PATH="/usr/lib/postgresql/${PG_VERSION}/bin:${PATH}"

    print_info "Using pg_config: $(which pg_config)"
    print_info "PostgreSQL version: $(pg_config --version)"
}

# ---------- step 2: create required symlinks ----------
create_symlinks() {
    print_header "Creating pgcommon/pgport symlinks"

    local PG_PKGLIBDIR
    PG_PKGLIBDIR="$(pg_config --pkglibdir)"
    local PG_LIBDIR
    PG_LIBDIR="$(pg_config --libdir)"

    # PGDG puts libpgcommon.a and libpgport.a in pkglibdir, but the linker
    # searches libdir. Create symlinks so PGXS builds find them.
    for lib in libpgcommon.a libpgport.a; do
        if [ -f "${PG_PKGLIBDIR}/${lib}" ] && [ ! -f "${PG_LIBDIR}/${lib}" ]; then
            sudo ln -sf "${PG_PKGLIBDIR}/${lib}" "${PG_LIBDIR}/${lib}"
            print_info "Symlinked ${lib}"
        fi
    done
}

# ---------- step 3: init submodules ----------
init_submodules() {
    print_header "Initializing git submodules"
    cd "${SCRIPT_DIR}"
    git submodule init
    git submodule update --recursive
}

# ---------- step 4: build avro ----------
build_avro() {
    print_header "Building Apache Avro"
    cd "${SCRIPT_DIR}"
    # avro target builds locally, install-avro copies into PG dirs (needs sudo)
    make avro
    sudo make install-avro
}

# ---------- step 5: build PG extensions ----------
build_pg_extensions() {
    print_header "Building PostgreSQL extensions"
    cd "${SCRIPT_DIR}"

    # Build order follows dependency chain
    local extensions=(
        pg_extension_base
        pg_map
        pg_extension_updater
        pg_lake_engine
        pg_lake_iceberg
        pg_lake_table
        pg_lake_copy
        pg_lake
    )

    for ext in "${extensions[@]}"; do
        print_info "Building ${ext}..."
        sudo make install-${ext}
    done
}

# ---------- step 6: build duckdb_pglake (no Azure) ----------
build_duckdb() {
    print_header "Building DuckDB (without Azure SDK)"
    cd "${SCRIPT_DIR}"

    # Build with ENABLE_AZURE=OFF, ENABLE_AWS_SDK=OFF, and no VCPKG_TOOLCHAIN_PATH.
    # DuckDB's httpfs extension provides built-in S3 support with key-based auth.
    # System OpenSSL and curl are found via cmake's find_package.
    make duckdb_pglake ENABLE_AZURE=OFF ENABLE_AWS_SDK=OFF VCPKG_TOOLCHAIN_PATH=""
    sudo make install-duckdb_pglake ENABLE_AZURE=OFF ENABLE_AWS_SDK=OFF VCPKG_TOOLCHAIN_PATH="" DUCKDB_BUILD_USE_CACHE=1
}

# ---------- step 7: build pgduck_server (no Azure) ----------
build_pgduck_server() {
    print_header "Building pgduck_server (without Azure)"
    cd "${SCRIPT_DIR}"

    # DUCKDB_BUILD_USE_CACHE=1 skips DuckDB rebuild (already installed above)
    sudo make install-pgduck_server PG_LAKE_AZURE_SUPPORT=0 PG_LAKE_AWS_SDK_SUPPORT=0 DUCKDB_BUILD_USE_CACHE=1
}

# ---------- step 8: verify installation ----------
verify_install() {
    print_header "Verifying installation"

    local PG_LIBDIR
    PG_LIBDIR="$(pg_config --libdir)"
    local PG_SHAREDIR
    PG_SHAREDIR="$(pg_config --sharedir)"
    local PG_BINDIR
    PG_BINDIR="$(pg_config --bindir)"

    local errors=0

    # Check shared libraries
    for lib in pg_lake_copy pg_lake_engine pg_lake_iceberg pg_lake_table pg_lake pg_extension_base pg_map pg_extension_updater; do
        if [ ! -f "${PG_LIBDIR}/${lib}.so" ]; then
            # Try pkglibdir
            local PG_PKGLIBDIR
            PG_PKGLIBDIR="$(pg_config --pkglibdir)"
            if [ ! -f "${PG_PKGLIBDIR}/${lib}.so" ]; then
                print_error "Missing: ${lib}.so"
                errors=$((errors + 1))
            fi
        fi
    done

    # Check DuckDB
    if [ ! -f "${PG_LIBDIR}/libduckdb.so" ]; then
        print_error "Missing: libduckdb.so"
        errors=$((errors + 1))
    fi

    # Check pgduck_server binary
    if ! command -v pgduck_server &>/dev/null; then
        if [ ! -f "${PG_BINDIR}/pgduck_server" ]; then
            print_error "Missing: pgduck_server binary"
            errors=$((errors + 1))
        fi
    fi

    # Check extension SQL files
    for ext in pg_lake_copy pg_lake_engine pg_lake_iceberg pg_lake_table pg_lake; do
        if [ ! -f "${PG_SHAREDIR}/extension/${ext}.control" ]; then
            print_error "Missing: ${ext}.control"
            errors=$((errors + 1))
        fi
    done

    if [ ${errors} -eq 0 ]; then
        print_header "All components installed successfully!"
    else
        print_error "${errors} component(s) missing"
        return 1
    fi
}

# ---------- summary ----------
print_summary() {
    print_header "Build complete!"
    print_elapsed

    local PG_BINDIR
    PG_BINDIR="$(pg_config --bindir)"

    cat <<EOF

Next steps:

  1. Ensure shared_preload_libraries includes pg_extension_base:
     echo "shared_preload_libraries = 'pg_extension_base'" >> \$(pg_config --sharedir)/postgresql.conf
     # Or edit the actual postgresql.conf file

  2. Restart PostgreSQL:
     sudo systemctl restart postgresql

  3. Create extensions:
     psql -c "CREATE EXTENSION pg_lake CASCADE;"

  4. Start pgduck_server:
     pgduck_server --cache_dir /tmp/cache &

  5. Test:
     psql -c "SELECT execute_in_pgduck('SELECT version()');"

Environment variables for this shell:
  export PATH="${PG_BINDIR}:\$PATH"

EOF
}

# ---------- main ----------
main() {
    print_header "pg_lake_copy build (no Azure SDK)"
    print_info "PostgreSQL version: ${PG_VERSION}"
    print_info "Build jobs: ${JOBS}"
    print_info "Source directory: ${SCRIPT_DIR}"
    echo

    install_system_deps
    create_symlinks
    init_submodules
    build_avro
    build_pg_extensions
    build_duckdb
    build_pgduck_server
    verify_install
    print_summary
}

main "$@"
