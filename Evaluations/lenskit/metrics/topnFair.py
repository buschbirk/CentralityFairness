"""
Fair Top-N evaluation metrics.


Lav measures ud fra : item, score, user, rank, algorithm, protected

"""

from __future__ import division
import random 
import numpy as np
import math
from .topn import *
#dataGenerator

from scipy.stats import spearmanr
from scipy.stats import pearsonr



ND_DIFFERENCE="rND" #represent normalized difference group fairness measure
KL_DIVERGENCE="rKL" #represent kl-divergence group fairness measure
RD_DIFFERENCE="rRD" #represent ratio difference group fairness measure
LOG_BASE=2 #log base used in logorithm function

NORM_CUTPOINT=10 # cut-off point used in normalizer computation
NORM_ITERATION=10 # max iterations used in normalizer computation
NORM_FILE="normalizer.txt" # externally text file for normalizers


##
def calculateNDFairnes(recs, truth, metric, protected_varible, providers=None):
    #print("calculateNDFairnes")
    #print(recs.head())
    #_ranking = []
    _ranking = recs['item'].tolist()
    #print("_ranking calculateNDFairness" )
    #print(_ranking)
    #print(" ")
    #pro_index=[idx for idx,row in _data.iterrows() if row[_sensi_att] == _sensi_bound]
    _protected_group_temp = recs.loc[recs[protected_varible] == 1]
    _protected_group = _protected_group_temp['item'].values
    _cut_point = 10 
    _gf_measure = metric
    
    if _gf_measure == "APCR":
        #print ("measure = div")
        return calculate_APCR(recs, providers)

    elif _gf_measure == "nd_APCR":
        #print ("measure = div")
        return calculate_nd_APCR(recs, providers, _cut_point)

    elif _gf_measure == "equal_ex":
        return calculate_equal_ex(recs, _protected_group)
    
    elif _gf_measure == "rND" or _gf_measure == "rKL" or _gf_measure == "rRD":
        #print ("calculate normalizer : ", len(recs), "+", len(_protected_group), "+", _gf_measure )
        #_normalizer = 1
        _normalizer = getNormalizer(len(recs), len(_protected_group), _gf_measure)
        print (_normalizer)
        return calculateNDFairnessPara(_ranking, _protected_group, _cut_point, _gf_measure, _normalizer, len(recs), len(_protected_group))
     
    elif _gf_measure == "ndcg":
        return ndcg(recs, truth)

    else:
        print( _gf_measure, "is not a valid _gf_measure")
        return 0; 
    

def calculate_equal_ex(recs, protected_group): 
    #beslut hvad der er nemmest. Skal denne tage "protected group". contains eller tage en protected variabel?
    
    exposure_pro = 0 
    count_pro = 0
    exposure_unpro = 0
    count_unpro = 0
    for index, row in recs.iterrows():
        if row["item"] in protected_group:
           exposure_pro = exposure_pro + (1/math.log2(1+row["rank"])) 
           count_pro += 1
        else:
           exposure_unpro = exposure_unpro + (1/math.log2(1+row["rank"]))  
           count_unpro += 1
    #return abs(exposure_pro-exposure_unpro)
    return (exposure_pro/count_pro)/(exposure_unpro/count_unpro)

def calculate_APCR(recs, providers):
    res1 = recs[providers]
    res2 = res1.sum()
    res3 = res2.loc[(res2>0)]
    
    return len(res3)/len(providers)

def calculate_nd_APCR(recs, providers, _cut_point):
    discounted_gf=0 #initialize the returned gf value
    normalizer = 0
    #print (recs.index)
    recs.reset_index()
    for countni in range(recs.shape[0]):
        countni=countni+1
        if(countni%_cut_point ==0):
            recs_cutpoint=recs.iloc[0:countni,:]
            #recs_cutpoint=recs.iloc[1:10,:]
            #print ( recs_cutpoint)
            #iteration_count +=1
            gf=calculate_APCR(recs_cutpoint,providers)
            #print("apcr: ", gf)
            #discounted_gf+=gf/math.log(countni+1,LOG_BASE) # log base -> global variable
            discounted_gf+=gf/(1.1**(countni-10/100))
            #print ("disc apcr = " , discounted_gf)
            #normalizer += 1/math.log(countni+1,LOG_BASE) 
            normalizer += 1/(1.1**(countni-10/100))
           

    return discounted_gf/normalizer


    

def calculateNDFairnessPara(_ranking, _protected_group, _cut_point, _gf_measure, _normalizer, items_n, proItems_n ):
    """
        Calculate group fairness value of the whole ranking.
        Calls function 'calculateFairness' in the calculation.
        :param _ranking: A permutation of N numbers (0..N-1) that represents a ranking of N individuals, 
                                e.g., [0, 3, 5, 2, 1, 4].  Each number is an identifier of an individual.
                                Stored as a python array.
        :param _protected_group: A set of identifiers from _ranking that represent members of the protected group
                                e.g., [0, 2, 3].  Stored as a python array for convenience, order does not matter.
        :param _cut_point: Cut range for the calculation of group fairness, e.g., 10, 20, 30,...
        :param _gf_measure:  Group fairness measure to be used in the calculation, 
                            one of 'rKL', 'rND', 'rRD'.
        :param _normalizer: The normalizer of the input _gf_measure that is computed externally for efficiency.
        :param
        :param 
        :return: returns  fairness value of _ranking, a float, normalized to [0, 1]
    """
    #print("calculateNDFairnessPara")
    #user_N=len(_ranking)
    #pro_N=len(_protected_group)

    if _normalizer==0:
        raise ValueError("Normalizer equals to zero")
     # error handling for input type
    if not isinstance(_ranking, (list, tuple, np.ndarray)) and not isinstance( _ranking, str ):
        raise TypeError("Input ranking must be a list-wise structure defined by '[]' symbol")
    if not isinstance(_protected_group, (list, tuple, np.ndarray)) and not isinstance( _protected_group, str ):
        raise TypeError("Input protected group must be a list-wise structure defined by '[]' symbol")
    if not isinstance( _cut_point, ( int ) ):
        raise TypeError("Input batch size must be an integer larger than 0")
    if not isinstance( _normalizer, (int, float, complex) ):
        raise TypeError("Input normalizer must be a number larger than 0")
    if not isinstance( _gf_measure, str ):
        raise TypeError("Input group fairness measure must be a string that choose from ['rKL', 'rND', 'rRD']")

    discounted_gf=0 #initialize the returned gf value
    for countni in range(len(_ranking)):
        countni=countni+1
        if(countni%_cut_point ==0):
            ranking_cutpoint=_ranking[0:countni]
            pro_cutpoint=set(ranking_cutpoint).intersection(_protected_group)
            gf=calculateFairness(ranking_cutpoint,pro_cutpoint,items_n, proItems_n,_gf_measure)
            #discounted_gf+=gf/math.log(countni+1,LOG_BASE) # log base -> global variable
            #print("counttni : ", countni)
            discounted_gf+=gf/(1.1**(countni-10/1000)) # log base -> global variable
            
            # make a call to compute, or look up, the normalizer; make sure to check that it's not 0!
            # generally, think about error handling

    
    return discounted_gf/_normalizer


def calculateFairness(_ranking,_protected_group,items_n, proItems_n,_gf_measure):
    """
        Calculate the group fairness value of input ranking.
        Called by function 'calculateNDFairness'.
        :param _ranking: A permutation of N numbers (0..N-1) that represents a ranking of N individuals, 
                                e.g., [0, 3, 5, 2, 1, 4].  Each number is an identifier of an individual.
                                Stored as a python array.
                                Can be a total ranking of input data or a partial ranking of input data.
        :param _protected_group: A set of identifiers from _ranking that represent members of the protected group
                                e.g., [0, 2, 3].  Stored as a python array for convenience, order does not matter.
        :param items_n: The size of input items 
        :param proItems_n: The size of input protected group
        :param _gf_measure: The group fairness measure to be used in calculation        
        :return: returns the value of selected group fairness measure of this input ranking
    """
      
    ranking_k=len(_ranking)
    pro_k=len(_protected_group)
    if _gf_measure==KL_DIVERGENCE: #for KL-divergence difference
        gf=calculaterKL(ranking_k,pro_k,items_n, proItems_n)        
        
    elif _gf_measure==ND_DIFFERENCE:#for normalized difference
        gf=calculaterND(ranking_k,pro_k,items_n, proItems_n)

    elif _gf_measure==RD_DIFFERENCE: #for ratio difference
        gf=calculaterRD(ranking_k,pro_k,items_n, proItems_n)     

    return gf 

def calculaterKL(_ranking_k,_pro_k,items_n, proItems_n):
    """
        Calculate the KL-divergence difference of input ranking        
        :param _ranking_k: A permutation of k numbers that represents a ranking of k individuals, 
                                e.g., [0, 3, 5, 2, 1, 4].  Each number is an identifier of an individual.
                                Stored as a python array.
                                Can be a total ranking of input data or a partial ranking of input data.
        :param _pro_k: A set of identifiers from _ranking_k that represent members of the protected group
                                e.g., [0, 2, 3].  Stored as a python array for convenience, order does not matter.
        :param items_n: The size of input items 
        :param proItems_n: The size of input protected group                
        :return: returns the value of KL-divergence difference of this input ranking
    """
    
    px=_pro_k/(_ranking_k)
    qx=proItems_n/items_n
     # manually set the value of extreme case to avoid error of math.log function
    if px == 0:
        px=0.001
    elif px == 1: 
       px = 0.999
    if qx == 0:
        qx=0.001
    elif qx == 1: 
        qx = 0.999
    return (px*math.log(px/qx,LOG_BASE)+(1-px)*math.log((1-px)/(1-qx),LOG_BASE))
#    rkl2 =px*math.log((px/qx),2)+(1-px)*math.log(((1-px)/(1-qx)),2)

def calculaterND(_ranking_k,_pro_k,items_n,proItems_n):
    """
        Calculate the normalized difference of input ranking        
        :param _ranking_k: A permutation of k numbers that represents a ranking of k individuals, 
                                e.g., [0, 3, 5, 2, 1, 4].  Each number is an identifier of an individual.
                                Stored as a python array.
                                Can be a total ranking of input data or a partial ranking of input data.
        :param _pro_k: A set of identifiers from _ranking_k that represent members of the protected group
                                e.g., [0, 2, 3].  Stored as a python array for convenience, order does not matter.
        :param items_n: The size of input items 
        :param proItems_n: The size of input protected group                
        :return: returns the value of normalized difference of this input ranking
    """
    #print("paramters : ", _ranking_k, _pro_k, items_n, proItems_n )
    #print("subresult : " , abs(_pro_k/_ranking_k-proItems_n/items_n))
    return abs(_pro_k/_ranking_k-proItems_n/items_n)

def calculaterRD(_ranking_k,_pro_k,items_n,proItems_n):
    """
        Calculate the ratio difference of input ranking        
        :param _ranking_k: A permutation of k numbers that represents a ranking of k individuals, 
                                e.g., [0, 3, 5, 2, 1, 4].  Each number is an identifier of an individual.
                                Stored as a python array.
                                Can be a total ranking of input data or a partial ranking of input data.
        :param _pro_k: A set of identifiers from _ranking_k that represent members of the protected group
                                e.g., [0, 2, 3].  Stored as a python array for convenience, order does not matter.
        :param items_n: The size of input items 
        :param proItems_n: The size of input protected group                
        :return: returns the value of ratio difference of this input ranking
        # This version of rRD is consistent with poster of FATML instead of arXiv submission.
    """
    # tjekker ikke om items_n-proItems_n er 0??? Det står i teksten 
    ## vores ændrign af koden der tjekker for 0: 
    unpro_n = items_n-proItems_n
    if unpro_n==0: # manually set the case of denominator equals zero
        input_ratio=0
    else:
        input_ratio=proItems_n/(unpro_n)
    
    ## den originale en linje kode erstattetet med ovenstående. 
    #input_ratio=proItems_n/(items_n-proItems_n)
    
    unpro_k=_ranking_k-_pro_k
    
    if unpro_k==0: # manually set the case of denominator equals zero
        current_ratio=0
    else:
        current_ratio=_pro_k/unpro_k

    min_ratio=min(input_ratio,current_ratio)
 
    return abs(min_ratio-input_ratio)

def getNormalizer(items_n,proItems_n,_gf_measure):
    """
        Retrieve the normalizer of the current setting in external normalizer dictionary.
        If not founded, call function 'calculateNormalizer' to calculate the normalizer of input group fairness measure at current setting.
        Called separately from fairness computation for efficiency.
        :param items_n: The total user number of input ranking
        :param proItems_n: The size of protected group in the input ranking        
        :param _gf_measure: The group fairness measure to be used in calculation
        
        :return: returns the maximum value of selected group fairness measure in _max_iter iterations
    """
    #print("normalizer:" , items_n, proItems_n) 
    # read the normalizor dictionary that is computed externally for efficiency
    normalizer_dic=readNormalizerDictionary()

    # error handling for type  
    if not isinstance( items_n, ( int ) ):
        raise TypeError("Input user number must be an integer")
    if not isinstance( proItems_n, ( int ) ):
        raise TypeError("Input size of protected group must be an integer")
    if not isinstance( _gf_measure, str ):
        raise TypeError("Input group fairness measure must be a string that choose from ['rKL', 'rND', 'rRD']")
    # error handling for value 
    if items_n <=0:
        raise ValueError("Input a valud user number")
    if proItems_n <=0:
        raise ValueError("Input a valid protected group size")
    if proItems_n >= items_n:
        raise ValueError("Input a valid protected group size")


    current_normalizer_key=str(items_n)+","+str(proItems_n)+","+_gf_measure
    if current_normalizer_key in normalizer_dic.keys():
        normalizer=normalizer_dic[current_normalizer_key]
    else:
        normalizer=calculateNormalizer(items_n,proItems_n,_gf_measure) 
        #print("normalizer: " , normalizer)
        #try:
        #    with open("normalizer.txt" , "a+") as f:                
        #        towrite = current_normalizer_key + ":" + str(normalizer)+"\n"
        #        f.write(towrite)
        #except EnvironmentError as e:
        #    print("Cannot find the normalizer txt file")          

   
    return float(normalizer)

def readNormalizerDictionary():
    """
        Retrieve recorded normalizer from external txt file that is computed external for efficiency.
        Normalizer file is a txt file that each row represents the normalizer of a combination of user number and protected group number.
        Has the format like this: user_N,pro_N,_gf_measure:normalizer
        Called by function 'getNormalizer'.
        :param : no parameter needed. The name of normalizer file is constant.     
        :return: returns normalizer dictionary computed externally.
    """
    try:
        with open("normalizer.txt") as f:
            lines = f.readlines()
    except EnvironmentError as e:
        print("Cannot find the normalizer txt file")
    
    
    normalizer_dic={}
    for line in lines:
        #print ("test: line in lines")
        normalizer=line.split(":")
        normalizer_dic[normalizer[0]]=normalizer[1]
    return normalizer_dic

def calculateNormalizer(items_n,proItems_n,_gf_measure):
    """
        Calculate the normalizer of input group fairness measure at input user and protected group setting.
        The function use two constant: NORM_ITERATION AND NORM_CUTPOINT to specify the max iteration and batch size used in the calculation.
        First, get the maximum value of input group fairness measure at different fairness probability.
        Run the above calculation NORM_ITERATION times.
        Then compute the average value of above results as the maximum value of each fairness probability.
        Finally, choose the maximum of value as the normalizer of this group fairness measure.
        
        :param items_n: The total user number of input ranking
        :param proItems_n: The size of protected group in the input ranking 
        :param _gf_measure: The group fairness measure to be used in calculation 
        
        :return: returns the group fairness value for the unfair ranking generated at input setting
    """
    #print ("calculating normalizer with userN=", items_n , ", proN", proItems_n , ", measure", _gf_measure )
    from lenskit.metrics.dataGenerator import generateUnfairRanking
    #import lenskit.metrics.dataGenerator * as dataGenerator  
    # set the range of fairness probability based on input group fairness measure
    if _gf_measure==RD_DIFFERENCE: # if the group fairness measure is rRD, then use 0.5 as normalization range
        f_probs=[0,0.5] 
    else:
        f_probs=[0,0.98] 
    avg_maximums=[] #initialize the lists of average results of all iteration
    for fpi in f_probs:
        iter_results=[] #initialize the lists of results of all iteration
        for iteri in range(NORM_ITERATION):
            input_ranking=[x for x in range(items_n)]
            protected_group=[x for x in range(proItems_n)]
            # generate unfair ranking using algorithm
            unfair_ranking=generateUnfairRanking(input_ranking,protected_group,fpi)    
            # calculate the non-normalized group fairness value i.e. input normalized value as 1
            gf=calculateNDFairnessPara(unfair_ranking,protected_group,NORM_CUTPOINT,_gf_measure,1, items_n, proItems_n)
            
            iter_results.append(gf)
        avg_maximums.append(np.mean(iter_results))        
    #print ("normalizer value to return : ", max(avg_maximums))
    return max(avg_maximums)

def calculateScoreDifference(_scores1,_scores2):
    """
        Calculate the average position-wise score difference
        between two sorted lists.  Lists are sorted in decreasing
        order of scores.  If lists are not sorted by descending- error.
        Only applied for two score lists with same size. 
        # check for no division by 0
        # check that each list is sorted in decreasing order of score
        :param _scores1: The first list of scores
        :param _scores2: The second list of scores         
        :return: returns the average score difference of two input score lists.
    """
    # error handling 
    if not isinstance(_scores1, (list, tuple, np.ndarray)) and not isinstance( _scores1, str ):
        raise TypeError("First score list must be a list-wise structure defined by '[]' symbol")
    if not isinstance(_scores2, (list, tuple, np.ndarray)) and not isinstance( _scores2, str ):
        raise TypeError("Second score list must be a list-wise structure defined by '[]' symbol")
    
    if len(_scores1)*len(_scores2) ==0:
        raise ValueError("Input score lists should have length larger than 0")
        
    if not descendingOrderCheck(_scores1):
        raise ValueError("First score list is not ordered by descending order")
    if not descendingOrderCheck(_scores2):
        raise ValueError("Second score list is not ordered by descending order")

    user_N=min(len(_scores1),len(_scores2)) # get the minimum user number of two score lists
    score_diff = 0
    for xi in range(user_N):        
        score_diff+=abs(_scores1[xi]-_scores2[xi])         
    score_diff=score_diff/user_N
    return score_diff

def calculatePositionDifference(_perm1,_perm2):
    """
        Calculate the average position difference for each item, 
        between two permutations of the same items.
        CHECK THAT EACH list is a valid permutation
        CHECK that lists are of the same size
        :param _perm1: The first permutation
        :param _perm2: The second permutation         
        :return: returns the average position difference of two input score lists.
    """
    completePermutaionCheck(_perm1,_perm2)
    user_N=len(_perm1) # get the total user number of two score list

    position_diff = 0
    positions_perm1=[]
    for ui in range(user_N):
        for pi in range(len(_perm1)):
            if ui==_perm1[pi]:
                positions_perm1.append(pi)
    positions_perm2=[]
    for ui in range(user_N):
        for pi in range(len(_perm2)):
            if ui==_perm2[pi]:
                positions_perm2.append(pi)    
    
    for i in range(user_N):        
        position_diff+=abs(positions_perm1[i]-positions_perm2[i])
    # get the average value of position difference
    if(user_N%2==0):
        position_diff=(2*position_diff)/(user_N*user_N)
    else:
        position_diff=(2*position_diff)/(user_N*user_N-1)
    return position_diff


# Functions for error handling
def descendingOrderCheck(_ordered_list):
    """
        Check whether the input list is ordered descending. 
        
        :param _ordered_list: The input list that is ordered by descending order         
        :return: returns true if order of _ordered_list is descending else returns false.
    """       
    return all(earlier >= later for earlier, later in zip(_ordered_list, _ordered_list[1:]))

def completePermutaionCheck(_perm1,_perm2):
    """
        Check the valid of two input permutations. 
        :param _perm1: The first permutation
        :param _perm2: The second permutation         
        :return: no returns. Raise error if founded.
    """
    
    if not isinstance(_perm1, (list, tuple, np.ndarray)) and not isinstance( _perm1, str ):
        raise TypeError("First permutation must be a list-wise structure defined by '[]' symbol")
    if not isinstance(_perm2, (list, tuple, np.ndarray)) and not isinstance( _perm2, str ):
        raise TypeError("Second permutation must be a list-wise structure defined by '[]' symbol")
    
    # error handling for complete permutation
    if len(_perm1)*len(_perm2) ==0:
        raise ValueError("Input permutations should have length larger than 0")
        
    if len(set(_perm1)) < len(_perm1):
        raise ValueError("First permutation include repetitive items")
    if len(set(_perm2)) < len(_perm2):
        raise ValueError("Second permutation include repetitive items")    
    if len(_perm1) != len(_perm2):
        raise ValueError("Input permutations should have same size")






