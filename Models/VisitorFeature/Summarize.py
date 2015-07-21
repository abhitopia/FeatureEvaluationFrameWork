from Experiment.Summarizer import Summarizer
import glob

for f_name in glob.glob("Data/*_result.store"):
    print 'Processing file:', f_name
    Summarizer(datafile=f_name).summarize()