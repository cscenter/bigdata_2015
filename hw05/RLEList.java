import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

public interface RLEList<T> {
	void append(T value);
	void insert(int index, T value);
	T get(int index);
	Iterator<T> iterator();

	class Demo {
		public static void main(String[] args) {
			RLEListImpl<Integer> rle = new RLEListImpl<Integer>();

			System.out.println("Lets check that initial RLE is empty:");
			System.out.println(rle.toString());

			System.out.println("\nNow lets append few values:");
			rle.append(1);
			rle.append(2);
			rle.append(3);
			rle.append(4);
			System.out.println(rle.toString());

			System.out.println("\nNow lets insert some value at position 0:");
			rle.insert(0, 42);
			System.out.println(rle.toString());
			System.out.println("And some value at position 3:");
			rle.insert(3, 42);
			System.out.println(rle.toString());

			System.out.println("\nSo now something harder: lets insert value 42 at position 0, so it would encode length:");
			rle.insert(0, 42);
			System.out.println(rle.toString());
			System.out.println("It may not be obvious by now, but actually we just consumed 0 additional memory!");
		}
	}
}

class RLEListRefImpl<T> implements RLEList<T> {
	private final ArrayList<T> myImpl = new ArrayList<>();

	public void append(T value) {
		myImpl.add(value);
	}

	public void insert(int index, T value) {
		myImpl.add(index, value);
	}

	public T get(int index) {
		return myImpl.get(index);
	}

	public Iterator<T> iterator() {
		return myImpl.iterator();
	}

}


class RLEListImpl<T> implements RLEList<T>, Iterable<T> {

    private Node root;
    private LinkedList<T> valuesList;


    /*
    Class constructor
     */
    public RLEListImpl () {
        root = null;
    }


    /*
    Class private methods
     */
    private class Node {
        public Node left, right;
        public int height, size;
        public T value;
        public int occurrences;

        public Node(T value) {
            this.left = null;
            this.right = null;
            this.size = 1;
            this.height = 1;
            this.value = value;
            this.occurrences = 1;
        }

        public void calcultate() {
            this.size = (this.left == null ? 0 : this.left.size) + (this.right == null ? 0 : this.right.size) + 1;
            this.height = Math.max((this.left == null ? 0 : this.left.height), (this.right == null ? 0 : this.right.height)) + 1;
        }
    }

    private T get(Node node, int i) {
        if (node == null) return null;

        if ((node.left == null ? 0 : node.left.size) > i) {
            return get(node.left, i);
        } else if (i >= (node.left == null ? 0 : node.left.size) + (node.occurrences - 1) + 1) {
            return get(node.right, i - (node.left == null ? 0 : node.left.size) - (node.occurrences - 1) - 1);
        } else {
            return node.value;
        }
    }

    private Node add(Node node, int i, T value) {
        if (node == null) return new Node(value);

        if (node.value == value) {
            if (((node.left == null ? 0 : node.left.size) <= i) && ((node.left == null ? 0 : node.left.size) + node.occurrences >= i))
            {
                node.occurrences++;
                return node;
            }
        }

        if ((node.left == null ? 0 : node.left.size) >= i) {
            node.left = add(node.left, i, value);
        } else {
            node.right = add(node.right, i - (node.left == null ? 0 : node.left.size) - (node.occurrences - 1) - 1, value);
        }
        node.calcultate();

        if (disbalance(node) >= 2) {
            if (disbalance(node.left) < 0) {
                node.left = rotateLeft(node.left);
            }
            node = rotateRight(node);
        } else if (disbalance(node) <= -2) {
            if (disbalance(node.right) > 0) {
                node.right = rotateRight(node.right);
            }
            node = rotateLeft(node);
        }

        return node;
    }

    // returns > 0 if left is bigger, < 0 if right is bigger, and 0 if equal
    private int disbalance(Node node) {
        return (node.left == null ? 0 : node.left.height) - (node.right == null ? 0 : node.right.height);
    }

    private Node rotateRight(Node node) {
        Node tmp = node.left;
        node.left = tmp.right;
        tmp.right = node;
        node.calcultate();
        tmp.calcultate();
        return tmp;
    }

    private Node rotateLeft(Node node) {
        Node tmp = node.right;
        node.right = tmp.left;
        tmp.left = node;
        node.calcultate();
        tmp.calcultate();
        return tmp;
    }

    private void explore(Node node) {
        if (node == null) return;
        explore(node.left);
        for (int i = 0; i < node.occurrences; i++) {
            valuesList.add(node.value);
        }
        explore(node.right);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<T> iter = this.iterator();
        sb.append("[");
        while (iter.hasNext()) {
            sb.append(iter.next() + ", ");
        }
        sb.append("]");
        return sb.toString();
    }



    /*
    Here are RLEList interface methods
     */
    @Override
    public void append(T value) {
        root = add(root, (root != null ? root.size : 0), value);
    }

    @Override
    public void insert(int index, T value) {
        root = add(root, index, value);
    }

    @Override
    public T get(int index) {
        return get(root, index);
    }




    /*
    Here is Iterable interface method
     */
    @Override
    public Iterator<T> iterator() {
        valuesList = new LinkedList<T>();
        explore(root);
        return valuesList.iterator();
    }

}