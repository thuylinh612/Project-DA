Data gồm có 2 bảng riêng biệt. Một bảng sẽ bao gồm lịch sử giá bán của các mặt hàng, bảng còn lại fact Sale.
Giá bán không cố định mà được update liên tục. 
Khi muốn tính doanh thu của một mặt hàng thì cần phải merge(join) id của order, mặt hàng với giá update mới nhất tương ứng với mặt hàng và thời gian order. 

