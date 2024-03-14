def splitBy(string,delimeters):
    """
    returns a list of the string separated by the delimeters given in the list 'delimeters'
    """
    result = []
    current = ''
    for char in string:
        jump = False
        for delimeter in delimeters:
            if char == delimeter:
                if char == '-':
                    continue
                if len(current) > 0:
                    result.append(current)
                    pass
                current = ''
                jump = True
                break
            pass
        if jump:
            continue
        current += char
        pass
    return result

def setQuery(query):
    """
    returns the query ready to use in the model
    """
    while query.startswith(' '):
        query = query[1:]
        pass
    while query.endswith(' '):
        query = query[:len(query) - 1]
        pass
    if query.count(' ') == 0:
        query += ' ' + query
        pass
    return query

def maxVectorValue(vector):
        m_v = vector[0]
        for v in vector:
            if v > m_v:
                m_v = v
                pass
            pass
        return m_v