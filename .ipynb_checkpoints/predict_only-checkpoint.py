{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "Using TensorFlow backend.\n"
     ]
    }
   ],
   "source": [
    "from keras.models import load_model\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [
    {
     "ename": "ImportError",
     "evalue": "No module named helper_with_N",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mImportError\u001b[0m                               Traceback (most recent call last)",
      "\u001b[0;32m<ipython-input-2-2d021d91e06f>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m()\u001b[0m\n\u001b[1;32m      6\u001b[0m \u001b[0;32mfrom\u001b[0m \u001b[0msklearn\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mmetrics\u001b[0m \u001b[0;32mimport\u001b[0m \u001b[0mconfusion_matrix\u001b[0m\u001b[0;34m,\u001b[0m\u001b[0mroc_auc_score\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m      7\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m----> 8\u001b[0;31m \u001b[0;32mfrom\u001b[0m \u001b[0mhelper_with_N\u001b[0m \u001b[0;32mimport\u001b[0m \u001b[0;34m*\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m      9\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     10\u001b[0m \u001b[0;32mimport\u001b[0m \u001b[0margparse\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;31mImportError\u001b[0m: No module named helper_with_N"
     ]
    }
   ],
   "source": [
    "from keras.models import Model#, Sequential\n",
    "from keras.layers import Input, Dense, Conv1D, concatenate, Dropout\n",
    "from keras.layers import GlobalMaxPooling1D,GlobalAveragePooling1D\n",
    "from keras.callbacks import ModelCheckpoint, Callback, LearningRateScheduler\n",
    "from keras.optimizers import Adam, Nadam,SGD\n",
    "from sklearn.metrics import confusion_matrix,roc_auc_score\n",
    "\n",
    "from helper_with_N import *\n",
    "\n",
    "import argparse\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 2",
   "language": "python",
   "name": "python2"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 2
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython2",
   "version": "2.7.15"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
