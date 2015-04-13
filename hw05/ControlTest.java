import java.util.Iterator;

public class ControlTest {
	private static RLEList<String> newInstance() {
		//return new TreeRLEList();
		throw new UnsupportedOperationException("Create instance of your class here");
	}

	private static int implLength(RLEList<String> l) {
		//return ((TreeRLEList)l).head.getTotalCount();
		return 0;
	}

	private static String concat(RLEList<String> l) {
		StringBuilder buf = new StringBuilder();
		for (Iterator<String> it = l.iterator(); it.hasNext();) {
			buf.append(it.next());
		}
		return buf.toString();
	}

	private static String implAsString(RLEList<String> l) {
		return "";
	}

	private static void assertContent(String expected, RLEList<String> l) {
		if (!expected.equals(concat(l))) {
			throw new IllegalArgumentException(String.format("Concat: expected=%s actual=%s", expected, concat(l)));
		} 
		for (int i = 0; i < expected.length(); i++) {
			String expectedItem = String.valueOf(expected.charAt(i));
			if (!expectedItem.equals(l.get(i))) {
				throw new IllegalArgumentException(String.format("get(%d): Expected:%s actual:%s", i, expectedItem, l.get(i)));
			} 
		}
	}

	private static void test1() {
	    RLEList<String> l = newInstance();
	    l.append("h");
	    l.append("e");
	    l.append("l");
	    l.append("l");
	    l.append("o");
	    assert 4 == implLength(l);
	    assertContent("hello", l);
	}



	private static void test2() {
	    RLEList<String> l = newInstance();
	    l.insert(0, "o");
	    l.insert(0, "l");
	    l.insert(0, "l");
	    l.insert(0, "e");
	    l.insert(0, "h");
	    assert 4 == implLength(l) : implAsString(l);
	    assertContent("hello", l);
	}


	private static void test3() {
	    RLEList<String> l = newInstance();
	    l.append("e");
	    l.append("l");
	    l.append("o");
	    l.insert(1, "l");
	    l.insert(0, "h");
	    assert 4 == implLength(l) : implAsString(l);
	    assertContent("hello", l);

	    l = newInstance();
	    l.append("e");
	    l.append("l");
	    l.append("o");
	    l.insert(2, "l");
	    l.insert(0, "h");
	    assert 4 == implLength(l) : implAsString(l);
	    System.err.println(implAsString(l));
	    assertContent("hello", l);
	}

	private static void test4() {
	    RLEList<String> l = newInstance();
	    l.append("h");
	    l.append("e");       
	    l.append("e");        
	    l.append("e");        
	    l.insert(2, "e");
	    l.insert(2, "E");
	    assert 4 == implLength(l) : implAsString(l);
	    assertContent("heEeee", l);

	    l = newInstance();
	    l.append("h");   
	    l.append("e");        
	    l.append("e");        
	    l.append("e");        
	    l.insert(2, "E");
	    l.insert(4, "E");

	    assert 6 == implLength(l) : implAsString(l);
	    assertContent("heEeEe", l);
	}

	private static void test5() {
	    RLEList<String> l = newInstance();
	    l.insert(0, "a");
	    l.insert(0, "a");
	    l.insert(0, "a");
	    assert 1 == implLength(l) : implAsString(l);
	    assertContent("aaa", l);
    }	
	public static void main(String[] args) {
		test1();
		test2();
		test3();
		test4();
		test5();
	}
}
