package boluo.model;

public class User {

	long userId;

	String name;

	int age;

	String gender;

	public User() {
	}

	public User(long userId, String name, int age, String gender) {
		this.userId = userId;
		this.name = name;
		this.age = age;
		this.gender = gender;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	@Override
	public String toString() {
		return "User{" +
				"userId=" + userId +
				", name='" + name + '\'' +
				", age=" + age +
				", gender='" + gender + '\'' +
				'}';
	}
}
