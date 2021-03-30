
top_srcdir = ..
srcdir = .

top_builddir = $(top_srcdir)
builddir = $(srcdir)

include $(top_builddir)/ThisIsWhatAutoconfIsForMakefile

target_name = MindsetMedia.Metrics.Plumber
target_type = library

target_src = $(srcdir)/Cache.fs $(srcdir)/Queue.fs $(srcdir)/Primitives.fs $(srcdir)/Ser.fs $(srcdir)/BinarySerialization.fs $(srcdir)/Serialization.fs $(srcdir)/Database.fs $(srcdir)/MetricsStorage.fs $(srcdir)/MetricsDb.fs $(srcdir)/QueueManager.fs $(srcdir)/Pipes.fs $(srcdir)/Printer.fs $(srcdir)/Join.fs $(srcdir)/Common.fs $(srcdir)/DbIO.fs $(srcdir)/CsvIO.fs $(srcdir)/IO.fs

target_refs = -r:System.dll -r:System.Configuration.dll -r:System.Core.Dll -r:System.Data.dll -r:System.Runtime.Serialization.dll -r:System.Xml.dll

include $(top_builddir)/TheseAreRulesAutoconfWouldGenerateMakefile

test::
