package DataEntity;

/**
 * @ClassName HotFuncItem
 * @Description:TODO
 * @Author Albert
 * Version v0.9
 */
public class HotFuncItem {
    private String funcId;
    private Long winEnd;
    private Long count;

    public HotFuncItem(String key, long end, Long next) {
        setFuncId(key);
        setWinEnd(end);
        setCount(next);

    }

    public String getFuncId() {
        return funcId;
    }

    public void setFuncId(String funcId) {
        this.funcId = funcId;
    }

    public Long getWinEnd() {
        return winEnd;
    }

    public void setWinEnd(Long winEnd) {
        this.winEnd = winEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
