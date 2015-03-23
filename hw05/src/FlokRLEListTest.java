import com.sun.xml.internal.ws.util.StringUtils;

import java.util.Iterator;

import static org.junit.Assert.*;

public class FlokRLEListTest {
    @org.junit.Test
    public void testAppend() throws Exception {
        FlokRLEList<Character> rle = new FlokRLEList<>();
        boolean equals = rleToString(rle).equals("");
        assertTrue(equals);

        rle.append('a');
        equals = rleToString(rle).equals("a");
        assertTrue(equals);

        rle = new FlokRLEList<>();
        rle.append('a');
        rle.append('a');
        rle.append('a');
        rle.append('b');
        rle.append('b');
        rle.append('c');
        equals = rleToString(rle).equals("aaabbc");
        assertTrue(equals);
    }

    @org.junit.Test
    public void testInsert() throws Exception {
        FlokRLEList<Character> rle = new FlokRLEList<>();
        rle.insert(0, 'a');
        boolean equals = rleToString(rle).equals("a");
        assertTrue(equals);

        rle.insert(1, 'a');
        equals = rleToString(rle).equals("aa");
        assertTrue(equals);

        rle.insert(2, 'a');
        equals = rleToString(rle).equals("aaa");
        assertTrue(equals);

        rle.insert(1, 'a');
        equals = rleToString(rle).equals("aaaa");
        assertTrue(equals);

        rle.insert(0, 'a');
        equals = rleToString(rle).equals("aaaaa");
        assertTrue(equals);

        rle.insert(0, 'b');
        equals = rleToString(rle).equals("baaaaa");
        assertTrue(equals);

        rle.insert(0, 'b');
        equals = rleToString(rle).equals("bbaaaaa");
        assertTrue(equals);

        rle.insert(1, 'b');
        equals = rleToString(rle).equals("bbbaaaaa");
        assertTrue(equals);

        rle.insert(1, 'b');
        equals = rleToString(rle).equals("bbbbaaaaa");
        assertTrue(equals);

        rle.insert(4, 'b');
        equals = rleToString(rle).equals("bbbbbaaaaa");
        assertTrue(equals);

        rle.insert(0, 'c');
        rle.insert(1, 'd');
        equals = rleToString(rle).equals("cdbbbbbaaaaa");
        assertTrue(equals);

        rle.insert(4, 'd');
        equals = rleToString(rle).equals("cdbbdbbbaaaaa");
        assertTrue(equals);

        rle.insert(8, 'd');
        equals = rleToString(rle).equals("cdbbdbbbdaaaaa");
        assertTrue(equals);

        rle.insert(8, 'd');
        equals = rleToString(rle).equals("cdbbdbbbddaaaaa");
        assertTrue(equals);

        rle.insert(10, 'd');
        equals = rleToString(rle).equals("cdbbdbbbdddaaaaa");
        assertTrue(equals);

        rle.insert(16, 'd');
        equals = rleToString(rle).equals("cdbbdbbbdddaaaaad");
        assertTrue(equals);

        rle.insert(16, 'd');
        equals = rleToString(rle).equals("cdbbdbbbdddaaaaadd");
        assertTrue(equals);

        rle.insert(18, 'd');
        equals = rleToString(rle).equals("cdbbdbbbdddaaaaaddd");
        assertTrue(equals);

    }

    @org.junit.Test(expected = IndexOutOfBoundsException.class)
    public void testInsertException() throws IndexOutOfBoundsException {
        FlokRLEList<Character> rle = new FlokRLEList<>();
        rle.insert(1, 'a');
    }

    @org.junit.Test(expected = IndexOutOfBoundsException.class)
    public void testInsertException2() throws IndexOutOfBoundsException {
        FlokRLEList<Character> rle = new FlokRLEList<>();
        rle.insert(-1, 'a');
    }


    @org.junit.Test
    public void testIterRemove() throws IndexOutOfBoundsException {
        FlokRLEList<Character> rle = new FlokRLEList<>();
        rle.append('a');
        rle.append('a');
        rle.append('a');

        Iterator<Character> it = rle.iterator();
        it.next();
        it.remove();
        boolean equals = rleToString(rle).equals("aa");
        assertTrue(equals);

        rle = new FlokRLEList<>();
        rle.append('a');
        it = rle.iterator();
        it.next();
        it.remove();
        equals = rleToString(rle).equals("");
        assertTrue(equals);

        //decreasing amount
        rle = new FlokRLEList<>();
        rle.append('a');
        rle.append('a');
        it = rle.iterator();
        it.next();
        it.next();
        it.remove();
        equals = rleToString(rle).equals("a");
        assertTrue(equals);

        //decreasing amont with switching interval
        rle = new FlokRLEList<>();
        rle.append('a');
        rle.append('a');
        rle.append('b');
        it = rle.iterator();
        it.next();
        it.next();
        it.remove();
        it.next();
        it.remove();
        equals = rleToString(rle).equals("a");
        assertTrue(equals);

        //gluing 2 intervals
        rle = new FlokRLEList<>();
        rle.append('a');
        rle.append('b');
        rle.append('a');
        it = rle.iterator();
        it.next();
        it.next();
        it.remove();
        equals = rleToString(rle).equals("aa");
        assertTrue(equals);
    }

    private String rleToString(RLEList<Character> rle) {
        StringBuilder sb = new StringBuilder();
        for(Character c : rle) {
            sb.append(c);
        }
        return sb.toString();
    }
}