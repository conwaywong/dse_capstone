import sys,traceback
import numpy as np

class s:
    """ compute the mean of matrices (have to be of same size) """
    def __init__(self,mat):
        self.reset(mat)

    def reset(self,mat):
        self.n = np.zeros(np.shape(mat))
        self.sum = np.zeros(np.shape(mat))

    def accum(self,value):
        """ Add a value to the statistics """

        if type(value) != np.ndarray:
            raise Exception('in s.accum: type of value='+str(type(value))+', it should be numpy.ndarray')

        if np.shape(value) != np.shape(self.sum):
            raise Exception('in s.accum: shape of value:'+str(np.shape(value))+\
                            ' is not equal to shape of sum:'+str(np.shape(self.sum)))

        self.sum += np.nan_to_num(value)
        self.n += (1-np.isnan(value))

    def compute(self):
        """ Returns the counts and the means for each entry """
        self.mean = self.sum / self.n
        self.mean = np.nan_to_num(self.mean)
        self.count = np.nan_to_num(self.n)
        return (self.count, self.mean)

    def add(self,other):
        """ add two statistics """
        self.n += other.n
        self.sum += other.sum

    def to_lists(self):
        return {'n':self.n.tolist(), 'sum':self.sum.tolist()}

    def from_lists(self,D):
        self.n = np.array(D['n'])
        self.sum = np.array(D['sum'])

class VecStat:
    """ Compute first and second order statistics of vectors of a fixed size n """
    def __init__(self,n):
        self.n = n
        self.reset()
        # Create a vector of length n and a matrix of size nXn

    def reset(self):
        n = self.n
        self.V = s(np.zeros(n))
        self.Cov = s(np.zeros([n,n]))

    def accum(self,U):
        """ accumulate statistics:
        U: an numpy array holding one vector
        """
        #check the length of U
        if len(U) != self.n :
            error='in Statistics.secOrdStat.accum: length of V='+str(self.n)+' not equal to length of U='+str(U.n)+'/n'
            sys.stderr.write(error)
            raise StandardError, error
        #check if U has the correct type
        if type(U) !=  np.ndarray:
            error='in Statistics.secOrdStat.accum: type of U='+str(type(U))+', it should be numpy.ndarray'
            sys.stderr.write(error)
            raise StandardError, error
        else:
            #do the work
            self.V.accum(U)
            self.Cov.accum(np.outer(U,U))

    def compute(self,k=5):
        """
        Compute the statistics. k (default 5) is the number of eigenvalues that are kept
        """

        # Compute mean vector
        (countV,meanV) = self.V.compute()

        # Compute covariance matrix
        (countC,meanC) = self.Cov.compute()
        cov = meanC - np.outer(meanV,meanV)
        std = [cov[i,i] for i in range(np.shape(self.Cov.sum)[0])]
        try:
            (eigvalues,eigvectors) = np.linalg.eig(cov)
            order = np.argsort(-abs(eigvalues))	# indexes of eigenvalues from largest to smallest
            eigvalues = eigvalues[order]		# order eigenvalues
            eigvectors = eigvectors[order]	    # order eigenvectors
            eigvectors = eigvectors[1:k]		# keep only top k eigen-vectors
            for v in eigvectors:
                v = v[order]     # order the elements in each eigenvector

        except Exception,e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,limit=2, file=sys.stderr)

            eigvalues = None
            eigvectors = None
        return {'count':self.V.n,'mean':meanV,'std':std,'eigvalues':eigvalues,'eigvectors':eigvectors}

    def add(self, other):
        """ add the statistics of s into self """
        self.V.add(other.V)
        self.Cov.add(other.Cov)

    def to_lists(self):
        return {'V':self.V.to_lists(),
                'Cov':self.Cov.to_lists()}

    def from_lists(self,D):
        self.V.from_lists(D['V'])
        self.Cov.from_lists(D['Cov'])
        self.n=len(self.V.sum)
