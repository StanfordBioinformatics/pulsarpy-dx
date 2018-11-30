#!/usr/bin/env python3

###
#Nathaniel Watson
#Stanford School of Medicine
#Nov. 28, 2018
#nathankw@stanford.edu
###

"""
"""

import argparse
import logging

import pulsarpy
import pulsarpy.utils


#The environment module gbsc/gbsc_dnanexus/current should also be loaded in order to log into DNAnexus

ENCODE_ORG = "org-snyder_encode"

def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-n", "--name", required=True, help="The name of the script that failed to run.")
    parser.add_argument("-t", "--to", nargs="+", default=pulsarpy.DEFAULT_TO, help="The recipient email address(s).")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    name = args.name
    recipients = args.to
    # Send email with error details to Admin
    body = "Oh la la."
    form = {
        "subject": "Script {} failed to run.".format(name),
        "text": body,
        "to": recipients,
    }
    res = pulsarpy.utils.send_mail(form=form, from_name="send_mail_if_script_norun.py")

if __name__ == "__main__":
    main()
