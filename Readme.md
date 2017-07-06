# gzlines
Sometimes you just want to iterate lines from GZ files.

This is a job that's made surprisingly awkward in stock Go, so I made some
helper functions.

Background: I like to store my data in JSON-Lines format, GZip-compressed. So
a typical workflow to use a dataset is to load every file in a directory,
gzip-decompress, and iterate them line-wise. In Python, this is trivial. In go,
it's a bit less fun.

This is awkward but trivial code. Consider it public domain.
If you need something more explicit, I hereby license it under the Creative Commons
Zero license in all jurisdictions in which I hold a right to copyright.
