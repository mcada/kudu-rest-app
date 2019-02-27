package io.syndesis.qe.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class Kudu {
	KuduClient client;
	String tableName = "my-table";

	public Kudu() {
		this.client = new KuduClient
				.KuduClientBuilder("syndesis-kudu:7051")
				.defaultOperationTimeoutMs(20000)
				.defaultSocketReadTimeoutMs(20000)
				.build();
	}

	public KuduClient getClient() {
		return this.client;
	}

	public void createTable() throws KuduException {
		// Set up a simple schema.
		List<ColumnSchema> columns = new ArrayList<>(2);
		columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
				.key(true)
				.build());
		columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(true)
				.build());
		Schema schema = new Schema(columns);

		// Set up the partition schema, which distributes rows to different tablets by hash.
		// Kudu also supports partitioning by key range. Hash and range partitioning can be combined.
		// For more information, see http://kudu.apache.org/docs/schema_design.html.
		CreateTableOptions cto = new CreateTableOptions();
		List<String> hashKeys = new ArrayList<>(1);
		hashKeys.add("key");
		int numBuckets = 2;
		cto.addHashPartitions(hashKeys, numBuckets);
		cto.setNumReplicas(1);

		// Create the table.
		client.createTable(tableName, schema, cto);
		System.out.println("Created table " + tableName);
	}

	public void deleteTable() throws KuduException {
		client.deleteTable(tableName);
	}

	/**
	 * @return true if successful
	 * @throws KuduException
	 */
	public boolean insertRows() throws KuduException {
		// Open the newly-created table and create a KuduSession.
		KuduTable table = client.openTable(tableName);
		KuduSession session = client.newSession();
		Insert insert = table.newInsert();
		PartialRow row = insert.getRow();

		row.addInt("key", 1);
		row.addString("value", "FirstValue");

		session.apply(insert);

		// Call session.close() to end the session and ensure the rows are
		// flushed and errors are returned.
		// You can also call session.flush() to do the same without ending the session.
		// When flushing in AUTO_FLUSH_BACKGROUND mode (the default mode recommended
		// for most workloads, you must check the pending errors as shown below, since
		// write operations are flushed to Kudu in background threads.
		session.close();
		if (session.countPendingErrors() != 0) {
			System.out.println("errors inserting rows");
			org.apache.kudu.client.RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
			org.apache.kudu.client.RowError[] errs = roStatus.getRowErrors();
			int numErrs = Math.min(errs.length, 5);
			System.out.println("there were errors inserting rows to Kudu");
			System.out.println("the first few errors follow:");
			for (int i = 0; i < numErrs; i++) {
				System.out.println(errs[i]);
			}
			if (roStatus.isOverflowed()) {
				System.out.println("error buffer overflowed: some errors were discarded");
			}
			return false;
		}
		return true;
	}

	public boolean scanTableAndCheckResults() throws KuduException {
		System.out.println("Scanning table..." + tableName);
		KuduTable table = client.openTable(tableName);
		Schema schema = table.getSchema();

		// Scan with a predicate on the 'key' column, returning the 'value' and "added" columns.
		List<String> projectColumns = new ArrayList<>(2);
		projectColumns.add("key");
		projectColumns.add("value");
		int lowerBound = 1;
		KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(
				schema.getColumn("key"),
				KuduPredicate.ComparisonOp.EQUAL,
				lowerBound);

		KuduScanner scanner = client.newScannerBuilder(table)
				.setProjectedColumnNames(projectColumns)
				.addPredicate(lowerPred)
				.build();

		// Check the correct number of values and null values are returned, and
		// that the default value was set for the new column on each row.
		// Note: scanning a hash-partitioned table will not return results in primary key order.
		int resultsFound = 0;
		while (scanner.hasMoreRows()) {
			RowResultIterator results = scanner.nextRows();
			while (results.hasNext()) {
				resultsFound++;
				RowResult result = results.next();
				System.out.println(result.toStringLongFormat());

				String value = result.getString("value");
				if (!value.equals("FirstValue")) {
					return false;
				}
			}
		}

		return resultsFound != 0;
	}
}
