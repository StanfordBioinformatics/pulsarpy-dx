# Pulsarpy DX
A Pulsarpy client extension for Pulsar LIMS that imports sequencing results from DNAnexus

AP documentation is on [Read the Docs](https://pulsarpy_dx.readthedocs.io/en/latest).

This is a companion tool to [Pulsarpy](https://github.com/nathankw/pulsarpy), which in turn is the offical
Python client for [Pulsar LIMS](https://github.com/nathankw/pulsar_lims). Pulsarpy DX is a gateway 
between Pulsar LIMS and related sequencing results stored on DNAnexus, and serves to 
import the sequencing results metadata from the DNAnexus platform into Pulsar LIMS. 

The main feature of this package is the script called ``import_seq_results.py``, which looks for projects under the specified DNAnexus billing org that were created within the past N days. Each project is checked if it's sequencing results need to be imoprted into Pulsar LIMS.  The way it works is quite specific to the laboratoy workflow of the Snyder Production Center of ENCODE, here at Stanford. 

## Lab workflow
A SequencingRequest record is made in Pulsar LIMS. Then, an Excel form is filled out and sent to the nearby Stanford Genome Sequencing Service Center (GSSC). That form contains the library name, which the lab personel set to be equal to the SequencingRequest record's name in Pulsar. This name in the Excel form will eventually make its way to a DNAnexus project as a property called `library_name` when GSSC uploads the sequencing results there. 

When ``import_seq_results.py`` is run, for each project it finds it will look at the value of the `library_name` property, then use this to look up a SequencingRequest by that name in Pulsar. The SequencingRequest record must pre-exist in order for sequencing results to be imported - a process that will entail creating a SequeningRun record and one or more SequencingResult records, as needed.  See the [script documentation](https://pulsarpy-dx.readthedocs.io/en/latest/scripts/import_seq_results.html) for more details. 
 
