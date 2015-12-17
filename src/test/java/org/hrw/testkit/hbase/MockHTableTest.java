package org.hrw.testkit.hbase;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * MockHTable Test.
 * <p/>
 * compare to real HTable.
 */

public class MockHTableTest {

    MockHTable mock;
    private String emailCF = "EMAIL";
    private String addressCF = "ADDRESS";
    private String emailCQ = "email";
    private String addressCQ = "address";
    private String tablePrefix = System.getProperty("user.name");
    private String tableName = tablePrefix + "-HTABLE-TEST";
    private HTableDescriptor tableDescriptor = createTableDescriptor(tableName, emailCF, addressCF);

    @Before

    public void setUp() throws Exception {
        mock = new MockHTable(tableName, emailCF, addressCF);

        setupDefaultData();
    }

    @After
    public void tearDown() throws Exception {
    }

    private void setupDefaultData() throws IOException, InterruptedException {
        Put john = createPutForPerson("testA", "testA.com", "US");
        Put jane = createPutForPerson("testB", "testB.email.com", "US");
        Put me = createPutForPerson("testC", "testC@gmail", "CN");
        mutateAll(john, jane, me);
    }

    private Put createPutForPerson(String name, String email, String address) {
        Put person = new Put(name.getBytes());
        person.add(emailCF.getBytes(), emailCQ.getBytes(), email.getBytes());
        person.add(addressCF.getBytes(), addressCQ.getBytes(), address.getBytes());
        return person;
    }

    private <R extends Row> void mutateAll(R... rows) throws IOException, InterruptedException {
        mock.batch(Lists.newArrayList(rows));
    }

    private HTableDescriptor createTableDescriptor(String tableName, String... cfs) {
        HTableDescriptor table = new HTableDescriptor(tableName);
        for (String cf : cfs) {
            table.addFamily(new HColumnDescriptor(cf));
        }
        return table;
    }

    @Test
    public void test_get() throws Exception {
        Get get = new Get("John Doe".getBytes());

        Result mockResult = mock.get(get);

        assertThat(mockResult.isEmpty(), is(mockResult.isEmpty()));
    }

    @Test
    public void test_get_with_filter() throws Exception {
        Get get = new Get("John Doe".getBytes());
        get.setFilter(new SingleColumnValueFilter(
                emailCF.getBytes(), emailCQ.getBytes(),
                CompareFilter.CompareOp.EQUAL, "WRONG EMAIL".getBytes()

        ));

        Result mockResult = mock.get(get);

        assertThat(mockResult.isEmpty(), is(mockResult.isEmpty()));
    }

//    @Test
//    public void test_exists() throws IOException {
//        Get get = new Get("John Doe".getBytes());
//        boolean mockResult = mock.exists(get);
//
//        assertThat(mockResult, is(true));
//        assertThat(mockResult, is(mockResult));
//    }

//    @Test
//    public void test_exists_include_not_exist_column() throws IOException {
//        Get get = new Get("John Doe".getBytes());
//        get.addColumn(emailCF.getBytes(), emailCQ.getBytes());
//        get.addColumn(emailCF.getBytes(), "NOT_EXIST_COLUMN".getBytes());
//        boolean mockResult = mock.exists(get);
//
//        assertThat(mockResult, is(true));
//        assertThat(mockResult, is(mockResult));
//    }

    @Test
    public void test_exists_with_only_not_exist_column() throws IOException {
        Get get = new Get("John Doe".getBytes());
        get.addColumn(emailCF.getBytes(), "NOT_EXIST_COLUMN".getBytes());
        boolean mockResult = mock.exists(get);

        assertThat(mockResult, is(false));
        assertThat(mockResult, is(mockResult));
    }

    @Test
    public void test_scan_with_filter() throws Exception {
        Scan scan = new Scan();
        scan.setFilter(new SingleColumnValueFilter(
                addressCF.getBytes(), addressCQ.getBytes(),
                CompareFilter.CompareOp.EQUAL, "US".getBytes()
        ));

        ResultScanner mockResult = mock.getScanner(scan);

        assertThat(Iterables.size(mockResult), is(2));
    }

//    @Test
//     public void test_scan_for_pre_match() throws Exception {
//        Scan scan = new Scan("J".getBytes(), "K".getBytes()); // start with 'J' only
//
//        ResultScanner mockResult = mock.getScanner(scan);
//
//        assertThat(Iterables.size(mockResult), is(2));
//    }
}