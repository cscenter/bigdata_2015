import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Random;


/**
 * RLEList implementation based on Cartesian tree a.k.a. Treap
 * All RLEList operation (append, insert, get) take O(log n) average time and amortized O(log n) time in worst case
 */
public class TreapRLEList<T> implements RLEList<T>, Iterable<T> {
	private Node root;
	private Random r;

	public TreapRLEList() {
		r = new Random();
	}

	private class Node {
		int key;
		int occurrences, size, sizeWithOccurrences;
		T data;
		Node left, right;
		Node parent;

		public Node(int key, T data) {
			this.key = key;
			this.data = data;
			this.occurrences = 1;
			fix();
		}

		boolean hasLeft() {
			return left != null;
		}

		boolean hasRight() {
			return right != null;
		}

		boolean hasParent() {
			return parent != null;
		}

		void fix() {
			size = size(left) + size(right) + 1;
			sizeWithOccurrences = sizeWithOccurrences(left) + sizeWithOccurrences(right) + occurrences;
			if (hasRight()) right.parent = this;
			if (hasLeft()) left.parent = this;
		}

		void fixPathToRoot() {
			fix();
			if (hasParent()) {
				parent.fixPathToRoot();
			}
		}

	}

	private class NodePair {
		Node left, right;

		void fix() {
			if (left != null) left.fix();
			if (right != null) right.fix();
		}
	}

	private Node getLeftmost(Node h) {
		if (!h.hasLeft()) {
			return h;
		}
		h.left.parent = h;
		if (h.hasRight()) {
			h.right.parent = h;
		}
		return getLeftmost(h.left);
	}

	/**
	 * Finds the next node
	 * Makes it possible to iterate through the tree without a stack
	 */
	private Node getSuccessor(Node h) {
		if (h.hasRight()) {
			h.right.parent = h;
			return getLeftmost(h.right);
		} else {
			while (h.hasParent() && h.parent.right == h) {
				h = h.parent;
			}
			return h.parent;
		}
	}

	/**
	 * Returns size of tree with root = h. Size of tree is calculated regardless of occurrences.
	 */
	private int size(Node h) {
		if (h == null) return 0;
		return h.size;
	}

	/**
	 * Returns size of tree with root = h. Size of tree is calculated with respect occurrences.
	 */
	private int sizeWithOccurrences(Node h) {
		if (h == null) return 0;
		return h.sizeWithOccurrences;
	}

	/**
	 * Merges two trees in a new one
	 */
	private Node merge(Node left, Node right) {
		if (left == null) {
			return right;
		}
		if (right == null) {
			return left;
		}
		if (left.key > right.key) {
			left.right = merge(left.right, right);
			left.fix();
			return left;
		} else {
			right.left = merge(left, right.left);
			right.fix();
			return right;
		}
	}

	/**
	 * Splits tree in a two trees - left one includes all nodes to the left of ith, other nodes go to the right one
	 */
	private NodePair split(Node h, int i) {
		NodePair hSplitted = new NodePair();
		if (h == null) {
			return hSplitted;
		}
		int sizeLeft = size(h.left);
		if (i <= sizeLeft) {
			NodePair leftSplitted = split(h.left, i);
			h.left = leftSplitted.right;
			hSplitted.left = leftSplitted.left;
			hSplitted.right = h;
		} else {
			NodePair rightSplitted = split(h.right, i - sizeLeft - 1);
			h.right = rightSplitted.left;
			hSplitted.right = rightSplitted.right;
			hSplitted.left = h;
		}
		hSplitted.fix();
		h.fix();
		return hSplitted;
	}

	/**
	 * Auxiliary class to make find method able to return three values instead of one
	 */
	private class SearchResult {
		Node node;
		int nodeIndex, nodeStartElementIndex;

		public SearchResult(Node node, int nodeIndex, int nodeStartElementIndex) {
			this.node = node;
			this.nodeIndex = nodeIndex;
			this.nodeStartElementIndex = nodeStartElementIndex;
		}
	}

	private Node insert(Node h, int i, T value) {
		Node toAdd = new Node(r.nextInt(), value);
		NodePair hSplitted = split(h, i);
		hSplitted.left = merge(hSplitted.left, toAdd);
		return merge(hSplitted.left, hSplitted.right);
	}

	private SearchResult find(Node h, int i) {
		int sizeLeft = sizeWithOccurrences(h.left);
		if (i <= sizeLeft) {
			return find(h.left, i);
		} else if (i <= sizeLeft + h.occurrences) {
			return new SearchResult(h, size(h.left) + 1, sizeWithOccurrences(h.left) + 1);
		} else {
			SearchResult result = find(h.right, i - sizeLeft - h.occurrences);
			result.nodeIndex += size(h.left) + 1;
			result.nodeStartElementIndex += sizeLeft + h.occurrences;
			return result;
		}
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	public int size() {
		return sizeWithOccurrences(root);
	}

	@Override
	public void append(T value) {
		if (isEmpty()) {
			root = insert(root, size(), value);
		} else {
			SearchResult result = find(root, size());
			if (value.equals(result.node.data)) {
				result.node.occurrences++;
				result.node.fixPathToRoot();
			} else {
				root = insert(root, size(), value);
			}
		}
	}

	@Override
	public void insert(int index, T value) {
		if (index < 0 || index > size()) {
			throw new IndexOutOfBoundsException(String.valueOf(index));
		}
		if (index == size()) {
			append(value);
			return;
		}
		index++;
		/**
		 * When inserting a value, there can be three scenarios:
		 * 1) value equals to ith element of the tree
		 *      In that case it's possible not to insert any new node,
		 *      but just increment corresponding 'occurrences' field
		 * 2) ith element of the tree is the first element of RLE block (i.e. node)
		 *      Just insert a new node to the left of RLE block
		 * 3) ith element is somewhere in the middle of RLE block
		 *      Split this block and insert a new node between them. Technically,
		 *      it's not a 'splitting' - we just decrease 'occurrences' field of block, and then
		 *      insert a two nodes - node with new value and right part of 'splitted' node.
		 */
		SearchResult result = find(root, index);
		if (result.node.data.equals(value)) {
			result.node.occurrences++;
			result.node.fixPathToRoot();
		} else if (index == result.nodeStartElementIndex) {
			root = insert(root, result.nodeIndex - 1, value);
		} else {
			root = insert(root, result.nodeIndex, value);
			root = insert(root, result.nodeIndex + 1, result.node.data);

			Node copy = find(root, index + 3).node;
			copy.occurrences = result.node.occurrences - index + result.nodeStartElementIndex;
			copy.fixPathToRoot();

			result.node.occurrences = index - result.nodeStartElementIndex;
			result.node.fixPathToRoot();
		}
	}

	@Override
	public T get(int index) {
		if (index < 0 || index >= size()) {
			throw new IndexOutOfBoundsException(String.valueOf(index));
		}
		return find(root, index + 1).node.data;
	}

	@Override
	public Iterator<T> iterator() {
		return new TreeRLEIterator();
	}


	/**
	 * Auxiliary class to implement {@link java.lang.Iterable} interface
	 */
	private class TreeRLEIterator implements Iterator<T> {
		int iterated, total;
		Node current;
		int iteratedWithinNode;

		TreeRLEIterator() {
			if (root != null) {
				this.current = getLeftmost(root);
			}
			this.total = size();
		}

		/**
		 * Checks if list was modified during iteration
		 * If was, {@link java.util.ConcurrentModificationException} should be thrown
		 *
		 * Not very good test, but it's okay in our case, because 'remove' operation is unsupported
		 * so the only way to change collection is to add a new element.
		 */
		void checkIfModified() {
			if (total != size()) {
				throw new ConcurrentModificationException();
			}
		}

		@Override
		public boolean hasNext() {
			checkIfModified();
			return iterated < total;
		}

		@Override
		public T next() {
			checkIfModified();
			if (iterated != 0 && iteratedWithinNode == current.occurrences) {
				current = getSuccessor(current);
				iteratedWithinNode = 0;
			}
			iteratedWithinNode++;
			iterated++;
			return current.data;
		}
	}

	@Override
	public String toString() {
		StringBuilder description = new StringBuilder("[");
		for (T t : this) {
			description.append(t.toString()).append(", ");
		}
		description.replace(description.length() - 2, description.length(), "]");
		return description.toString();
	}

	/**
	 * Some unit tests
	 */
	public static void main(String[] args) {
		TreapRLEList<String> list = new TreapRLEList<>();
		assert list.isEmpty();

		Iterator<String> iter = list.iterator();
		assert !iter.hasNext();

		ArrayList<String> arrayList = new ArrayList<>(
				Arrays.asList(new String[]{"a", "a", "a", "b", "b", "b", "c", "c", "c"}));
		arrayList.forEach(list::append);
		assert list.size() == arrayList.size();
		assert list.find(list.root, list.size()).nodeIndex == 3; // checks if it's actually only 3 nodes in tree

		arrayList.add(1, "d");
		arrayList.add(1, "d");
		list.insert(1, "d");
		list.insert(1, "d");
		assert list.size() == arrayList.size();
		assert list.find(list.root, list.size()).nodeIndex == 5;


		iter = list.iterator();
		list.append("c");
		arrayList.add("c");
		boolean wasThrown = false;
		try {
			iter.next();
		} catch (ConcurrentModificationException e) {
			wasThrown = true;
		}
		assert wasThrown;

		assert list.toString().equals(arrayList.toString());

		System.out.println(list);
	}
}