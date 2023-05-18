from aced_submission.simplifier import get_oid


def test_oid_lookup():
    """Ensure we can find names for oid"""
    oids = ['2.16.840.1.113883.2.4.6.3', '2.16.840.1.113883.6.117', '2.16.840.1.113883.19.5']
    bad_oids = ['1.2.3.4.5', '0.1.2.3.4.5.6.7', ]

    for _ in oids:
        name, content = get_oid(_)
        assert name, (_, content)

    for _ in bad_oids:
        name, content = get_oid(_)
        assert name is None, (_, name, content)
