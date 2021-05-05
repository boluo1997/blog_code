package boluo.basics;

public enum Note02_Enum {

    PROVINCE(1, "省"),
    CITY(2, "市"),
    PREFECTURE(3, "县"),
    TOWNSHIP(4, "乡镇"),
    VILLAGE(5, "村庄");

    private int type;
    private String name;

    Note02_Enum(int type, String name) {
        this.type = type;
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean eq(Integer val) {
        if (val == null) {
            return false;
        }

        return val.equals(type);
    }

}
