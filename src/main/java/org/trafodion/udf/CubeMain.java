package org.trafodion.udf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CubeMain {

    public static void main(String[] args) {
        // JdbcConnectionInfo t2 = new JdbcConnectionInfo();
        // Connection conn = t2.getConnection();
        // System.out.println(conn);
        // if (conn != null) {
        // PreparedStatement ps =
        // conn.prepareStatement("select city,dept from t2");
        // if (ps.execute()) {
        // System.out.println("execute");
        // }
        // }
        CUBE cube = new CUBE();
        test(cube);
    }

    static List<Object[]> genVirtualResult(List<String> colList) {
        List<Object[]> city_dept = new ArrayList<Object[]>();
        Object[] arr = { 18000, 6000, "BJ", "3" };
        Object[] arr2 = { 15000, 5000, "BJ", "2" };
        Object[] arr3 = { 12000, 4000, "BJ", "1" };
        Object[] arr4 = { 3600, 1800, "SH", "3" };
        Object[] arr5 = { 3400, 1700, "SH", "2" };
        Object[] arr6 = { 3200, 1600, "SH", "1" };
        city_dept.add(arr);
        city_dept.add(arr2);
        city_dept.add(arr3);
        city_dept.add(arr4);
        city_dept.add(arr5);
        city_dept.add(arr6);
        List<Object[]> city = new ArrayList<Object[]>();
        Object[] arr7 = { 45000, 5000, "BJ" };
        Object[] arr8 = { 10200, 1700, "SH" };
        city.add(arr7);
        city.add(arr8);
        List<Object[]> dept = new ArrayList<Object[]>();
        Object[] arr9 = { 21600, 4320, "3" };
        Object[] arr10 = { 18400, 3680, "2" };
        Object[] arr11 = { 15200, 3040, "1" };
        dept.add(arr9);
        dept.add(arr10);
        dept.add(arr11);

        if (colList.contains("city") && colList.contains("dept")) {
            return city_dept;
        } else if (colList.contains("city")) {
            return city;
        } else if (colList.contains("dept")) {
            return dept;
        }
        return null;
    }

    static void test(CUBE cube) {
        cube.results.clear();
        String[] par = { "select sal,city,dept from t2", "sum(sal), avg(sal)", "city,       dept" };
        String[] out = { "A", "B", "city", "dept" };
        int numCols = out.length;

        String query = par[0];
        String aggreCol = par[1];

        List<String> groupByCols = Arrays.asList(par[2].replaceAll(" ", "").split(","));

        for (int i = 1; i <= groupByCols.size(); i++)
            cube.combinerSelect(groupByCols, new ArrayList<String>(), groupByCols.size(), i, cube.results);

        List<String> rsCols = null;
        Map<String, Integer> map = null;

        for (List<String> colList : cube.results) {
            rsCols = cube.getAggreCol(aggreCol);
            rsCols.addAll(colList);
            colList.add(0, aggreCol);
            String realQuery = cube.parseQuery(query, colList.toArray(new String[colList.size()]));
            System.out.println(realQuery);
            map = cube.genColAndIndexMap(rsCols);

            List<Object[]> rs = genVirtualResult(colList);
            Iterator<Object[]> iterator = rs.iterator();
            while (iterator.hasNext()) {
                Object[] data = iterator.next();
                for (int c = 0; c < numCols; c++) {
                    String colName = out[c];

                    if (map.containsKey(colName)) {
                        String val = data[map.get(colName) - 1].toString();
                        System.out.print(val + "\t");
                    }
                }
                System.out.println();
            }
        }
    }
}
