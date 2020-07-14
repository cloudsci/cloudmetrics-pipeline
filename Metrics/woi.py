#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from .utils import findFiles, getField
import pywt


class WOI():
    '''
    Class for computing Wavelet Organisation Indices, as proposed by Brune et 
    al. (2018) from a cloud water path field. Can compute the three indices
    WOI1, WOI2 and WOI3 suggested in that paper.
    
    Parameters
    ----------
    mpar : Dict
       Specifies the following parameters:
           loadPath : Path to load .h5 files that contain a pandas dataframe
                      with a cloud mask field as one of the columns.
           savePath : Path to a .h5 containing a pandas dataframe whose columns
                      contain metrics and whose indices are scenes. This class 
                      can fill the columns 'woi1', 'woi2', 'woi3' and 'woi'.
           save     : Boolean to specify whether to store the variables in
                      savePath/Metrics.h5
           resFac   : Resolution factor (e.g. 0.5), to coarse-grain the field.
           plot     : Boolean to specify whether to make plot with details on
                      this metric for each scene.
           con      : Connectivitiy for segmentation (1 - 4 seg, 2 - 8 seg)
           areaMin  : Minimum cloud size considered in computing metric
           fMin     : First scene to load
           fMax     : Last scene to load. If None, is last scene in set.
                     
    '''
    def __init__(self, mpar):
        # General parameters
        self.loadPath = mpar['loadPath']
        self.savePath = mpar['savePath']
        self.save     = mpar['save']
        self.saveExt  = mpar['saveExt']
        self.resFac   = mpar['resFac']
        self.plot     = mpar['plot']
        self.con      = mpar['con']
        self.areaMin  = mpar['areaMin']
        self.fMin     = mpar['fMin']
        self.fMax     = mpar['fMax']

        # Metric-specific parameters
        self.field    = 'Cloud_Water_Path'
        self.fieldRef = 'Cloud_Mask_1km'

    def metric(self,field,cm,verify=False):
        '''
        Compute metric(s) for a single field

        Parameters
        ----------
        field : numpy array of shape (npx,npx) - npx is number of pixels
            Cloud water path field.
        cm : numpy array of shape (npx,npx) 
            Cloud mask field.

        Returns
        -------
        D0 : float
            Mean geometric nearest neighbour distance between objects.
        scai : float
            Simple Convective Aggregation Index.

        '''
        
        # STATIONARY/UNDECIMATED Direct Wavelet Transform
        scaleMax = int(np.log(field.shape[0])/np.log(2))
        coeffs = pywt.swt2(field,'haar',scaleMax,norm=True,trim_approx=True)
        # Bug in pywt -> trim_approx=False does opposite of its intention
        # Structure of coeffs:
        # - coeffs    -> list with nScales indices. Each scale is a 2-power of 
        #                the image resolution. For 512x512 images we have
        #                512 = 2^9 -> 10 scales
        # - coeffs[i] -> Contains three directions:
        #                   [0] - Horizontal
        #                   [1] - Vertical
        #                   [2] - Diagonal
        
        specs = np.zeros((len(coeffs),3))  # Shape (nScales,3)
        k = np.arange(0,len(specs))
        for i in range(len(coeffs)):
            if i == 0:
                ec = coeffs[i]**2
                specs[i,0] = np.mean(ec)
            else:
                for j in range(len(coeffs[i])):
                    ec = coeffs[i][j]**2     # Energy -> squared wavelet coeffs
                    specs[i,j] = np.mean(ec) # Domain-averaging at each scale     
        
        # Decompose into ''large scale'' energy and ''small scale'' energy
        # Large scales are defined as 0 < k < 5
        specs = specs[1:]
        specL = specs[:5,:]
        specS = specs[5:,:]
        
        Ebar  = np.sum(np.mean(specs,axis=1))
        Elbar = np.sum(np.mean(specL,axis=1))
        Esbar = np.sum(np.mean(specS,axis=1))
        
        Eld    = np.sum(specL,axis=0)
        Esd    = np.sum(specS,axis=0)
        
        # Compute wavelet organisation index
        woi1 = Elbar / Ebar
        woi2 = (Elbar + Esbar) / np.sum(cm)
        woi3 = 1./3*np.sqrt(np.sum(((Esd - Esbar)/Esbar)**2 + ((Eld - Elbar)/Elbar)**2))
        
        woi  = np.log(woi1) + np.log(woi2) + np.log(woi3)
        
        if self.plot:
            labs = ['Horizontal','Vertical','Diagonal']
            fig,axs = plt.subplots(ncols=2,figsize=(8,4))
            axs[0].imshow(field,'gist_ncar'); 
            axs[0].set_xticks([]); axs[0].set_yticks([])
            axs[0].set_title('CWP')
            for i in range(3):
                axs[1].plot(k[1:],specs[:,i],label=labs[i])
            axs[1].set_xscale('log')
            axs[1].set_xlabel(r'Scale number $k$')
            axs[1].set_ylabel('Energy')
            axs[1].set_title('Wavelet energy spectrum')
            axs[1].legend()
            plt.tight_layout()
            plt.show()
    
        if verify:
            return specs
        else:
            return woi1, woi2, woi3, woi
    
    def verify(self):
        '''
        Verify that the wavelet energy spectrum contains the same energy as the
        original field

        '''
        files, _ = findFiles(self.loadPath)
        file = files[self.fMin]
        cwp = getField(file, self.field,    self.resFac, binary=False)
        cm  = getField(file, self.fieldRef, self.resFac, binary=True)
        
        specs = self.metric(cwp,cm,verify=True)
        
        # Validate wavelet energy spectrum -> if correct total energy should be 
        # the same as in image space
        Ewav = np.sum(specs)
        Eimg = np.mean(cwp**2)
        
        diff = Ewav - Eimg
        if diff < 1e-10:
            print('Energy conserved by SWT')
        else:
            print('Energy not conserved by SWT - results will be wrong')
                
    def compute(self):
        '''
        Main loop over scenes. Loads fields, computes metric, and stores it.

        '''
        files, dates = findFiles(self.loadPath)
        files = files[self.fMin:self.fMax]
        dates = dates[self.fMin:self.fMax]

        if self.save:
            saveSt    = self.saveExt
            dfMetrics = pd.read_hdf(self.savePath+'/Metrics'+saveSt+'.h5')
        
        ## Main loop over files
        for f in range(len(files)):
            cwp = getField(files[f], self.field, self.resFac, binary=False)
            cm  = getField(files[f], self.fieldRef, self.resFac, binary=True)
            print('Scene: '+files[f]+', '+str(f+1)+'/'+str(len(files)))
            
            woi1, woi2, woi3, woi = self.metric(cwp,cm)
            print('woi1 = ',woi1)
            print('woi2 = ',woi2)
            print('woi3 = ',woi3)
            print('woi  = ',woi )

            if self.save:
                dfMetrics['woi'].loc[dates[f]]  = woi
                dfMetrics['woi1'].loc[dates[f]] = woi1
                dfMetrics['woi2'].loc[dates[f]] = woi2
                dfMetrics['woi3'].loc[dates[f]] = woi3
        
        if self.save:
            dfMetrics.to_hdf(self.savePath+'/Metrics'+saveSt+'.h5', 'Metrics',
                             mode='w')

if  __name__ == '__main__':
    mpar = {
            'loadPath' : '/Users/martinjanssens/Documents/Wageningen/Patterns-in-satellite-images/testEnv/Data/Filtered',
            'savePath' : '/Users/martinjanssens/Documents/Wageningen/Patterns-in-satellite-images/testEnv/Data/Metrics',
            'save'     : True, 
            'resFac'   : 1,     # Resolution factor (e.g. 0.5)
            'plot'     : True,  # Plot with details on each metric computation
            'con'      : 1,     # Connectivity for segmentation (1:4 seg, 2:8 seg)
            'areaMin'  : 4,     # Minimum cloud size considered for object metrics
            'fMin'     : 0,     # First scene to load
            'fMax'     : None,  # Last scene to load. If None, is last scene in set
           }
    woiGen = WOI(mpar)
    woiGen.verify()
    woiGen.compute()
    
