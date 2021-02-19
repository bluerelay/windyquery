class TextVal:
    reserved = {}
    tokens = ('TEXTVAL',)

    t_TEXTVAL = r"('|E')(?:.(?!(?<!')'(?!')))*.?'"
