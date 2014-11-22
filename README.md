hdps
====

This is an (actively developing) implementation of the High Dimensional
Propensity score algorithm in the R language. It is part of the OHDSI project
(see ohdsi.org and the OHDSI/* github repositories) and depends upon much of
the code which has been written by that team.

This code developed out of (and sometimes directly borrows) code written by
OHDSI (especially by Patrick Ryan, Martijn Schuemie, and Marc Suchard).

Overview of HDPS
----

The HDPS algorithm is used to compute propensity scores in patient data. The
main point of the algorithm is to do variable selection in order to cut down
the (often) large amount of variables found in patient data to ones most suited
for the propensity score computations. The algorithm is described here:

Schneeweiss S, Rassen JA, Glynn RJ, Avorn J, Mogun H, Brookhart MA.
"High-dimensional propensity score adjustment in studies of treatment effects
using health care claims data." _Epidemiology_ 2009;20:512-22


Overview of this software
----

The implementation is meant to target the OMOP Common Data Model (see
omop.org), but is designed so that that isn't required (though of course the
user is then responsible for fitting the software into their model). There are
essentially two pieces to the software: (1) data generators and (2)
covariate generators.

The data generators are responsible for interfacing with the OMOP CDM database
and downloading the necessary patient data for the algorithm. This is the
portion one would need to rewrite in order to target another data model. In
addition to the OMOP CDM data generators, there is also a data similator. The
purpose of the similator is not to produce data that is representative of real
patient data, but instead simply to show the user how the data should be
structured for the covariate generators. This is useful both for those
targeting other data models as well as for those who want to better want to
step through and understand the algorithm.

The covariate generators are responsible for converting the patient data
according to the HDPS algorithm.


Installing and using the software
-----

See INSTALL for details on how to install the software. See the examples/
folder for example files to run.
