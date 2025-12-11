FIXED_ROUTES = [
    "/",
    "/home",
    "/about",
    "/carian-sekolah",
    "/siaran"
]

def build_snap_routes(docs):
    routes = list(FIXED_ROUTES)
    for doc in docs:
        code = doc.get("KODSEKOLAH") or str(doc.get("_id"))
        routes.append(f"/halaman-sekolah/{code}")
    return routes