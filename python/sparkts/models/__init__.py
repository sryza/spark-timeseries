
def _py2java_int_array(vals, gw):
    result = gw.new_array(gw.jvm.int, len(vals))
    for i in range(0, len(vals)):
        result[i] = int(vals[i])
    return result

def _py2java_double_array(vals, gw):
    result = gw.new_array(gw.jvm.double, len(vals))
    for i in range(0, len(vals)):
        result[i] = float(vals[i])
    return result

def _nparray2breezevector(arr, sc=None):
    return sc._jvm.breeze.linalg.DenseVector(_py2java_double_array(arr, sc._gateway))
    
def _nparray2breezematrix(arr, sc=None):
    (rows, cols) = arr.shape
    return sc._jvm.breeze.linalg.DenseMatrix(rows, cols, _py2java_double_array(arr.flatten(), sc._gateway))

def _py2scala_seq(vals, gw):
    return gw.jvm.com.cloudera.sparkts.PythonConnector.arrayListToSeq(vals)
