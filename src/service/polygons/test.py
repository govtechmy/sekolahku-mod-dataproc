from src.service.polygons.load_opendosm_negeri import calculate_centroid
from src.models.negeriEnum import NegeriEnum

print("Running centroid calculation test:")
print(calculate_centroid(NegeriEnum.PERAK))