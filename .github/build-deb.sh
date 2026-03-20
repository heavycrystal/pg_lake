#!/bin/bash
set -euo pipefail

# Usage: build-deb.sh <pg_version> <deb_arch>
PG_VERSION="$1"
DEB_ARCH="$2"

PG_LIBDIR="$(pg_config --libdir)"
PG_PKGLIBDIR="$(pg_config --pkglibdir)"
PG_SHAREDIR="$(pg_config --sharedir)"
PG_BINDIR="$(pg_config --bindir)"

BASE_VERSION=$(grep 'default_version' pg_lake/pg_lake.control | sed "s/.*'\(.*\)'/\1/")
if [[ "${GITHUB_REF:-}" == refs/tags/v* ]]; then
  TAG_VERSION="${GITHUB_REF#refs/tags/v}"
  PKG_VERSION="${BASE_VERSION}+tag.${TAG_VERSION}"
else
  PKG_VERSION="${BASE_VERSION}~dev.${GITHUB_SHA:0:8}"
fi

PKG_NAME="pg-lake-copy-pg${PG_VERSION}"
DEB_FILE="${PKG_NAME}_${PKG_VERSION}-1_${DEB_ARCH}.deb"
STAGING="/tmp/${PKG_NAME}_${PKG_VERSION}-1_${DEB_ARCH}"
rm -rf "${STAGING}"

# Extension shared libraries
mkdir -p "${STAGING}${PG_PKGLIBDIR}"
for ext in pg_lake_copy pg_lake_engine pg_lake_iceberg pg_lake_table pg_lake \
           pg_extension_base pg_map pg_extension_updater; do
  cp "${PG_PKGLIBDIR}/${ext}.so" "${STAGING}${PG_PKGLIBDIR}/" 2>/dev/null || \
  cp "${PG_LIBDIR}/${ext}.so" "${STAGING}${PG_PKGLIBDIR}/" 2>/dev/null || true
done

# DuckDB and avro shared libraries
mkdir -p "${STAGING}${PG_LIBDIR}"
cp "${PG_LIBDIR}/libduckdb.so" "${STAGING}${PG_LIBDIR}/"
cp "${PG_LIBDIR}/libavro.so" "${STAGING}${PG_LIBDIR}/libavro.so.24.0.1"
ln -sf libavro.so.24.0.1 "${STAGING}${PG_LIBDIR}/libavro.so.24"
ln -sf libavro.so.24.0.1 "${STAGING}${PG_LIBDIR}/libavro.so"

# Extension control and SQL files
mkdir -p "${STAGING}${PG_SHAREDIR}/extension"
for ext in pg_lake_copy pg_lake_engine pg_lake_iceberg pg_lake_table pg_lake \
           pg_extension_base pg_map pg_extension_updater; do
  cp "${PG_SHAREDIR}/extension/${ext}"* "${STAGING}${PG_SHAREDIR}/extension/" 2>/dev/null || true
done

# pgduck_server binary
mkdir -p "${STAGING}${PG_BINDIR}"
cp "${PG_BINDIR}/pgduck_server" "${STAGING}${PG_BINDIR}/"

# systemd service
mkdir -p "${STAGING}/lib/systemd/system"
cat > "${STAGING}/lib/systemd/system/pgduck-server@.service" <<'EOF'
[Unit]
Description=pgduck_server for PostgreSQL %i
After=postgresql@%i.service
PartOf=postgresql@%i.service

[Service]
Type=simple
User=postgres
ExecStart=/bin/sh -c 'exec /usr/lib/postgresql/$(echo %i | cut -d- -f1)/bin/pgduck_server'
Restart=on-failure
RestartSec=5

[Install]
WantedBy=postgresql@%i.service
EOF

# postinst
mkdir -p "${STAGING}/DEBIAN"
cat > "${STAGING}/DEBIAN/postinst" <<EOF
#!/bin/sh
set -e
systemctl daemon-reload || true
systemctl enable pgduck-server@${PG_VERSION}-main || true
systemctl start pgduck-server@${PG_VERSION}-main || true
EOF
chmod 755 "${STAGING}/DEBIAN/postinst"

# Strip debug info
find "${STAGING}" \( -name '*.so' -o -name '*.so.*' -o -name 'pgduck_server' \) -type f -exec strip {} +
echo "Stripped sizes:"
find "${STAGING}" \( -name '*.so' -o -name '*.so.*' -o -name 'pgduck_server' \) -type f -exec ls -lh {} +

# DEBIAN control
mkdir -p "${STAGING}/DEBIAN"
cat > "${STAGING}/DEBIAN/control" <<EOF
Package: ${PKG_NAME}
Version: ${PKG_VERSION}-1
Section: database
Priority: optional
Architecture: ${DEB_ARCH}
Depends: postgresql-${PG_VERSION}, libssl3 (>= 3.0), libcurl4 (>= 7.68), libjansson4, zlib1g, libstdc++6, libpq5, libnuma1
Maintainer: pg_lake contributors
Homepage: https://github.com/snowflake-labs/pg_lake
Description: pg_lake data-lake extensions for PostgreSQL ${PG_VERSION} (S3-only, no Azure)
 Includes pg_lake_copy, pg_lake_table, pg_lake_iceberg, pg_lake_engine,
 pgduck_server (DuckDB query engine), and all dependency extensions.
 Built without Azure SDK — S3 and GCS supported via DuckDB httpfs.
EOF

dpkg-deb --build --root-owner-group "${STAGING}" "${DEB_FILE}"
ls -lh "${DEB_FILE}"

echo "DEB_FILE=${DEB_FILE}" >> "${GITHUB_ENV:-/dev/null}"
