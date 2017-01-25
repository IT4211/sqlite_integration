import argparse
import os

def ParseCommandLine():
    parser = argparse.ArgumentParser('sqlite join')

    parser.add_argument('-p', '--path', type=ValidateDirectory, require=True, help="Specify the directory where the sqlite db file exists")

    parser.add_argument('-o', '--output', type=ValidateDirectory, require=True, help="Specify the output directory")

    gl_args = parser.parse_args()

    return gl_args

def ValidateDirectory(theDir):
    if not os.path.isdir(theDir):
        raise argparse.ArgumentTypeError('Directory does not exist')

    if os.access(theDir, os.R_OK):
        return theDir
    else:
        raise argparse.ArgumentTypeError('Directory is not readable')