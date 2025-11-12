# tests/test_o09_international_names.py
from src.ingest.normalize import normalize_name_parts


def test_particles_kept_in_surname():
    first_norm, last_norm, _ = normalize_name_parts("Jean de La Fontaine")
    assert first_norm == "jean"
    assert last_norm == "de la fontaine"


def test_iberian_multiword_particle():
    first_norm, last_norm, _ = normalize_name_parts("Ana María de la Torre")
    assert first_norm.startswith("ana") and "maria" in first_norm  # tolerant
    assert last_norm == "de la torre"


def test_diacritics_removed():
    first_norm, last_norm, _ = normalize_name_parts("María-José Carreño Quiñones")
    assert "maria" in first_norm and "jose" in first_norm
    assert "qu" in last_norm and "nones" in last_norm  # quinones (diacritic stripped)


def test_cjk_family_first_basic():
    first_norm, last_norm, _ = normalize_name_parts("王 小明")
    assert first_norm in {"xiaoming", "xiao ming"}  # unidecode can vary tokenization
    assert last_norm == "wang"
