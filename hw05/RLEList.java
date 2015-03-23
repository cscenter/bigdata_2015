import java.util.ArrayList;
import java.util.Iterator;

public interface RLEList<T> {
	void append(T value);
	void insert(int index, T value);
	T get(int index);
	Iterator<T> iterator();

	class Demo {
		public static void main(String[] args) {
			RLEList<String> list = new RLEListRefImpl<>();
			list.append("foo");
			list.insert(0, "bar");
			System.out.println(list.iterator().next());
			System.out.println(list.get(1));
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