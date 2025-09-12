from datetime import datetime

def required_columns():
    return [
        "NEGERI",
        "PPD",
        "PARLIMEN",
        "DUN",
        "PERINGKAT",
        "JENIS/LABEL",
        "KODSEKOLAH",
        "NAMASEKOLAH",
        "ALAMATSURAT",
        "POSKODSURAT",
        "BANDARSURAT",
        "NOTELEFON",
        "NOFAX",
        "EMAIL",
        "LOKASI",
        "GRED",
        "BANTUAN",
        "BILSESI",
        "SESI",
        "ENROLMEN PRASEKOLAH",
        "ENROLMEN",
        "ENROLMEN KHAS",
        "GURU",
        "PRASEKOLAH",
        "INTEGRASI",
        "KOORDINATXX",
        "KOORDINATYY",
        "SKM<=150",
    ]

def transform_row(r, context):
    def int_or_none(value):
        try:
            if value in (None, "", " "):
                return None
            return int(value)
        except (ValueError, TypeError):
            return None

    def float_or_none(value):
        try:
            if value in (None, "", " "):
                return None
            return float(value)
        except (ValueError, TypeError):
            return None

    return {
        "_id": r["KODSEKOLAH"],
        "kodsekolah": r["KODSEKOLAH"],
        "namasekolah": r["NAMASEKOLAH"],

        "administration": {
            "negeri": r.get("NEGERI"),
            "ppd": r.get("PPD"),
            "parlimen": r.get("PARLIMEN"),
            "dun": r.get("DUN")
        },

        "peringkat": r.get("PERINGKAT"),
        "jenis_label": r.get("JENIS/LABEL"),
        "gred": r.get("GRED"),
        "bantuan": r.get("BANTUAN"),

        "address": {
            "alamatsurat": r.get("ALAMATSURAT"),
            "poskodsurat": int_or_none(r.get("POSKODSURAT")),
            "bandarsurat": r.get("BANDARSURAT")
        },

        "contact": {
            "notelefon": str(r.get("NOTELEFON") or "").strip() or None,
            "nofax": str(r.get("NOFAX") or "").strip() or None,
            "email": r.get("EMAIL")
        },

        "lokasi": r.get("LOKASI"),

        "coordinates": {
            "type": "Point",
            "coordinates": [
                float_or_none(r.get("KOORDINATXX")),
                float_or_none(r.get("KOORDINATYY"))
            ]
        },

        "sessions": {
            "bilsesi": r.get("BILSESI"),
            "sesi": r.get("SESI")
        },

        "enrolment": {
            "enrolmen_prasekolah": int_or_none(r.get("ENROLMEN PRASEKOLAH")),
            "enrolmen": int_or_none(r.get("ENROLMEN")),
            "enrolmen_khas": int_or_none(r.get("ENROLMEN KHAS")),
            "skm_150": str(r.get("SKM<=150", "")).strip().upper() == "YA"
        },

        "teachers": {
            "guru": int_or_none(r.get("GURU")),
            "prasekolah": r.get("PRASEKOLAH"),
            "integrasi": r.get("INTEGRASI")
        },

        "metadata": {
            "run_id": context.run_id,
            "ingestiondate": datetime.utcnow()
        }
    }

def validate_row(doc, context):
    """Custom validation for schools dataset"""
    errors, warnings = [], []
    _id = doc.get("_id")

    # ---- Critical checks ----
    if not _id:
        errors.append("Missing _id")

    enrol = doc.get("enrolment", {})
    if not isinstance(enrol.get("enrolmen"), (int, type(None))):
        errors.append(f"Invalid enrolmen value -> {enrol.get('enrolmen')}")

    teachers = doc.get("teachers", {})
    if not isinstance(teachers.get("guru"), (int, type(None))):
        errors.append(f"Invalid teachers.guru -> {teachers.get('guru')}")

    coords = doc.get("coordinates", {}).get("coordinates", [None, None])
    if coords[0] is None or coords[1] is None:
        warnings.append(f"Missing coordinates")
    elif not all(isinstance(c, float) for c in coords):
        errors.append(f"Bad coordinates -> {coords}")

    email = doc.get("contact", {}).get("email")
    if not email or email in ("-", "--", "TIADA"):
        warnings.append(f"Invalid/missing email -> {email}")

    return errors, warnings
