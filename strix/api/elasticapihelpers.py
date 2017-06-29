
def page_size(from_hit=0, to_hit=None, limit=True, size=None):
    if limit:
        if (to_hit and to_hit > 10000) or (size and from_hit and size + from_hit > 10000):
            raise RuntimeError("Paging error. \"to\" cannot be larger than 10000")
    if to_hit and from_hit and (to_hit < from_hit):
        raise RuntimeError("Paging error. \"to\" cannot be smaller than \"from\"")

    if size is not None:
        return {
            "from": from_hit,
            "to": from_hit + size
        }
    if to_hit is not None:
        return {
            "from": from_hit,
            "to": to_hit
        }
    return {
        "from": from_hit,
        "to": from_hit + 10
    }
