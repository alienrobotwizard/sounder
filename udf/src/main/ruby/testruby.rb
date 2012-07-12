require 'pigudf'
class Myudfs < PigUdf
  def square num
    return nil if num.nil?
    num**2
  end
end
