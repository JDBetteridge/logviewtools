# PETSc -logview plot and profile

Tools for examining the profile of PETSc applications.
This used the output of the PETSc application run with `-logview` to generate plots.

Internally the logview is converted to a Pandas dataframe.
Matplotlib is then used to visualise the data.
