package io.syndesis.qe.kudu;

import org.apache.kudu.client.KuduException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kudu/table")
public class KuduController {
	@Autowired
	Kudu kudu;

	@RequestMapping("/insert")
	public String insertIntoTable() {
		try {
			if (kudu.insertRows()) {
				return "Insert successful";
			} else {
				return "Insert failed";
			}
		} catch (KuduException e) {
			return "Kudu error happened during row insertion: " + e;
		}
	}

	@RequestMapping("/create")
	public String createTable() {
		try {
			kudu.createTable();
		} catch (KuduException e) {
			System.out.println(e);
			return "Table creation failed, exception: " + e;
		}
		return "Table created";
	}

	@RequestMapping("/delete")
	public String deleteTable() {
		try {
			kudu.deleteTable();
		} catch (KuduException e) {
			return "An error was thrown when deleting a table: " + e;
		}
		return "Table deleted";
	}

	@RequestMapping("/validate")
	public String checkTableContent() {
		try {
			if (kudu.scanTableAndCheckResults()) {
				return "Success";
			} else {
				return "Fail";
			}
		} catch (KuduException e) {
			return "Kudu exception happened: " + e;
		}
	}
}
