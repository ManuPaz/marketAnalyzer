import numpy as np
cimport numpy as np
np.import_array()
cpdef  np.ndarray diff_to_absolute(np.ndarray data, int diff):
    cdef int i
    data[0:diff] = 1
    for i in range(diff,len(data)):
        if not np.isnan(data[i]):
            data[i] = data[i - diff] * (1 + 0.01 * data[i])
        else:
            data[i]=data[i - diff]
    return data
