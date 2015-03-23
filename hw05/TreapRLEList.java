import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Random;

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
			if (hasParent()) parent.fixPathToRoot();
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

	private int size(Node h) {
		if (h == null) return 0;
		return h.size;
	}

	private int sizeWithOccurrences(Node h) {
		if (h == null) return 0;
		return h.sizeWithOccurrences;
	}

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

	public int size() {
		return sizeWithOccurrences(root);
	}

	@Override
	public void append(T value) {
		if (size() == 0) {
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
			throw new IndexOutOfBoundsException(String.format("%d should be in [0, %d]", index, size()));
		}
		if (index == size()) {
			append(value);
		} else {
			index++;
			SearchResult result = find(root, index);
			if (result.node.data.equals(value)) {
				result.node.occurrences++;
				result.node.fixPathToRoot();
			} else if (index == result.nodeStartElementIndex) {
				root = insert(root, result.nodeIndex - 1, value);
			} else {
				System.out.println("hey");
				root = insert(root, result.nodeIndex, value);
				root = insert(root, result.nodeIndex + 1, result.node.data);
				Node copy = find(root, index + 2).node;
				copy.occurrences = result.node.occurrences - index + result.nodeStartElementIndex;
				copy.fixPathToRoot();
				result.node.occurrences = index - result.nodeStartElementIndex;
				result.node.fixPathToRoot();
			}
		}
	}

	@Override
	public T get(int index) {
		if (index < 0 || index >= size()) {
			throw new IndexOutOfBoundsException(String.format("%d should be in [0, %d)", index, size()));
		}
		return find(root, index + 1).node.data;
	}

	@Override
	public Iterator<T> iterator() {
		return new TreeRLEIterator();
	}


	private class TreeRLEIterator implements Iterator<T> {
		int iterated, total;
		Node current;
		int iteratedWithinNode;

		TreeRLEIterator() {
			this.current = getLeftmost(root);
			this.total = size();
		}

		void checkIfModified() {
			// Not very good consistency test, but it's okay in our case, because there's no
			// 'remove' operation so the only way to change collection is to add a new element.
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
}
