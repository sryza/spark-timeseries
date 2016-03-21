
def _py2java_int_array(sc, vals):
    gw = sc._gateway
    result = gw.new_array(gw.jvm.int, len(vals))
    for i in range(0, len(vals)):
        result[i] = int(vals[i])
    return result

def _py2java_double_array(sc, vals):
    gw = sc._gateway
    result = gw.new_array(gw.jvm.double, len(vals))
    for i in range(0, len(vals)):
        result[i] = float(vals[i])
    return result

def _nparray2breezevector(sc, arr):
    return sc._jvm.breeze.linalg.DenseVector(_py2java_double_array(sc, arr))
    
def _nparray2breezematrix(sc, arr):
    (rows, cols) = arr.shape
    return sc._jvm.breeze.linalg.DenseMatrix(rows, cols, _py2java_double_array(sc, arr.flatten()))

def _py2scala_seq(sc, vals):
    gw = sc._gateway
    return gw.jvm.com.cloudera.sparkts.PythonConnector.arrayListToSeq(vals)
