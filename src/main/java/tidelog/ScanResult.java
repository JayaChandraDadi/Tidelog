package tidelog;

import java.util.List;

/** Rows returned by a scan plus how many data files were read versus skipped. */
public record ScanResult(List<Row> rows, int filesTotal, int filesScanned, int filesSkipped) {}
