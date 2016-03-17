
def data_file_as_dense_vector(fn):
    with open(fn) as f:
        lines = f.readlines()
    values = [float(l) for l in lines]
    return Vectors.dense(values)


