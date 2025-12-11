def build_school_list(docs):
    mapping = []

    for doc in docs:
        key = doc.get("kodSekolah")
        value = doc.get("namaSekolah")

        if key and value:
            mapping.append({"KODSEKOLAH": key, "NAMASEKOLAH": value})

    return mapping