import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Random;

public class CartesianRLEList<T> implements RLEList<T>, Iterable<T> {
	private Node root;
	private Random r;

	public CartesianRLEList() {
		r = new Random();
	}

	private class Node {
		int key;
		int occurrences, size;
		T data;
		Node left, right;
		Node parent;  // useful only in iteration, so correctness of this reference is not supported all the time

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
			size = size(left) + size(right) + occurrences;
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

	private Node insert(Node h, int i, T value) {
		Node toAdd = new Node(r.nextInt(), value);
		NodePair hSplitted = split(h, i);
		hSplitted.left = merge(hSplitted.left, toAdd);
		return merge(hSplitted.left, hSplitted.right);
	}

	private Node find(Node h, int i) {
		int sizeLeft = size(h.left);
		if (i <= sizeLeft) {
			return find(h.left, i);
		} else if (i <= sizeLeft + h.occurrences) {
			return h;
		} else {
			return find(h.right, i - sizeLeft - h.occurrences);
		}
	}


	public int size() {
		return size(root);
	}

	@Override
	public void append(T value) {
		root = insert(root, size(root), value);
	}

	@Override
	public void insert(int index, T value) {
		root = insert(root, index, value);
	}

	@Override
	public T get(int index) {
		return find(root, index + 1).data;
	}

	@Override
	public Iterator<T> iterator() {
		return new TreeRLEIterator(root);
	}

	private class TreeRLEIterator implements Iterator<T> {
		int iterated, total;
		Node current;

		TreeRLEIterator(Node root) {
			this.current = getLeftmost(root);
			this.total = size(root);
		}


		void checkIfModified() {
			// Not very good consistency test, but it's okay in our case, because there's no
			// 'remove' operation so the only way to change collection is to add a new element.
			if (total != size(CartesianRLEList.this.root)) {
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
			if (iterated != 0) {
				current = getSuccessor(current);
			}
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
