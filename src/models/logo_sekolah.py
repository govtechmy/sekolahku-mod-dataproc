from __future__ import annotations

from pydantic import BaseModel, Field


class LogoSekolah(BaseModel):
	"""Model for logo sekolah documents in MongoDB collection 'LogoSekolah'.

	Source CSV: data/tbi_institusi_induk.csv

	Columns:
	- KOD_INSTITUSI (string)
	- NAMA_PENUH_INSTITUSI (string)
	- LOGO (string, base64 encoded)
	"""

	kod_institusi: str = Field(..., alias="KOD_INSTITUSI", description="Kod sekolah")
	nama_penuh_institusi: str = Field(..., alias="NAMA_PENUH_INSTITUSI", description="Full name of the school")
	logo: str = Field(..., alias="LOGO", description="Base64 encoded image string")

	def mongo_document(self) -> dict:
		"""Return a dict ready to be inserted/upserted into MongoDB.

		Uses KOD_INSTITUSI as the natural key and also as _id.
		"""

		return {
			"_id": self.kod_institusi,
			"KOD_INSTITUSI": self.kod_institusi,
			"NAMA_PENUH_INSTITUSI": self.nama_penuh_institusi,
			"LOGO": self.logo,
		}

